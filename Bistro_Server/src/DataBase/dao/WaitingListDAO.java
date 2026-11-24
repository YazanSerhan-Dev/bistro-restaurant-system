package DataBase.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
import common.dto.SubscriberDTO;
import common.dto.TerminalValidateResponseDTO;
import common.dto.WaitingListDTO;
/**
 * Data Access Object for waiting list management.
 * <p>
 * Handles waiting-list lifecycle including creation, assignment,
 * check-in, cancellation, and table reservation coordination.
 * Used by dashboard, terminal, and background schedulers.
 */
public class WaitingListDAO {

    // =============================================================
    // 1. DASHBOARD METHODS
    // =============================================================
	/**
	 * Inserts a new waiting-list entry.
	 *
	 * @param numOfCustomers number of customers in the party
	 * @param requestTime time the request was made
	 * @param status initial status (WAITING / ASSIGNED)
	 * @param confirmationCode unique confirmation code
	 * @throws Exception on database error
	 */
    public static void insertWaiting(
            int numOfCustomers,
            Timestamp requestTime,
            String status,
            String confirmationCode) throws Exception {

        String sql = """
                INSERT INTO waiting_list
                (num_of_customers, request_time, status, confirmation_code)
                VALUES (?, ?, ?, ?)
            """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, numOfCustomers);
            ps.setTimestamp(2, requestTime);
            ps.setString(3, status);
            ps.setString(4, confirmationCode);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Retrieves all active waiting-list entries.
     * <p>
     * Includes only WAITING and ASSIGNED statuses,
     * ordered by request time for dashboard display.
     *
     * @return list of waiting-list DTOs
     */
    public static List<WaitingListDTO> getAllWaitingList() throws Exception {
        List<WaitingListDTO> list = new ArrayList<>();

        String sql = "SELECT waiting_id, num_of_customers, request_time, status, confirmation_code FROM waiting_list WHERE status IN ('WAITING', 'ASSIGNED') ORDER BY request_time ASC";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        // try-with-resources + finally: the statement/result set are always closed and the
        // connection always goes back to the pool (before, an exception leaked all three).
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                int id = rs.getInt("waiting_id");
                int count = rs.getInt("num_of_customers");
                String status = rs.getString("status");
                String code = rs.getString("confirmation_code");

                java.sql.Timestamp ts = rs.getTimestamp("request_time");
                String timeStr = (ts != null) ? ts.toString() : "";

                list.add(new WaitingListDTO(id, count, timeStr, status, code));
            }
        } finally {
            pool.releaseConnection(pc);
        }
        return list;
    }

    /** Result of joining the waiting list. */
    public static final class JoinResult {
        /** waiting_id of the new entry (its confirmation code is also used to pay the bill). */
        public final int waitingId;
        /** Table the customer was sent to right away, or null if they are now waiting. */
        public final String seatedTableId;

        JoinResult(int waitingId, String seatedTableId) {
            this.waitingId = waitingId;
            this.seatedTableId = seatedTableId;
        }
    }

    /**
     * Joins the waiting list in ONE transaction: inserts the entry and its user_activity row
     * (so an entry never exists without contact details), then - as the spec requires - if a
     * suitable table is free right now, sends the customer straight to it instead of waiting.
     *
     * @param numOfCustomers     party size
     * @param requestTime        request time
     * @param confirmationCode   unique waiting code
     * @param subscriberUsername subscriber username, or null for a guest
     * @param guestEmail         guest email (null for subscribers)
     * @param guestPhone         guest phone (null for subscribers)
     * @return the new waiting_id, and the table if the customer was seated immediately
     * @throws java.sql.SQLIntegrityConstraintViolationException if the code already exists
     * @throws Exception on other database errors
     */
    public static JoinResult insertWaitingWithActivity(int numOfCustomers, Timestamp requestTime, String confirmationCode,
                                                       String subscriberUsername, String guestEmail, String guestPhone)
            throws Exception {

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            int waitingId;
            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO waiting_list (num_of_customers, request_time, status, confirmation_code)
                    VALUES (?, ?, 'WAITING', ?)
                """, java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, numOfCustomers);
                ps.setTimestamp(2, requestTime);
                ps.setString(3, confirmationCode);
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (!rs.next()) throw new java.sql.SQLException("No waiting_id generated.");
                    waitingId = rs.getInt(1);
                }
            }

            int activityId = UserActivityDAO.insertActivity(conn, subscriberUsername, guestPhone, guestEmail,
                    null, waitingId, requestTime);

            String tableId = (activityId > 0) ? trySeatImmediately(conn, waitingId, numOfCustomers, activityId) : null;

            conn.commit();
            return new JoinResult(waitingId, tableId);

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }

    /**
     * Spec: "if a table is available immediately, the customer is sent to the table right away,
     * instead of the notification and without entering the waiting list".
     * <p>
     * Takes the smallest free table that fits, but only if:
     * <ul>
     *   <li>nobody who joined earlier and fits that table is still waiting (they keep their turn), and</li>
     *   <li>the table is not needed by reservations due in the next 2 hours.</li>
     * </ul>
     * On success the table is OCCUPIED, a visit is started and the entry is ARRIVED.
     * Otherwise nothing changes (savepoint) and the customer stays WAITING.
     *
     * @return the table id, or null if the customer must wait
     */
    private static String trySeatImmediately(Connection conn, int waitingId, int people, int activityId) throws Exception {
        Savepoint sp = conn.setSavepoint("SEAT_NOW");

        String tableId = RestaurantTableDAO.reserveFreeTableForWaitingReturnTableId(conn, waitingId, people);
        if (tableId == null) { conn.rollback(sp); return null; }

        int tableSeats = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT num_of_seats FROM restaurant_table WHERE table_id = ?")) {
            ps.setString(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) tableSeats = rs.getInt(1);
            }
        }

        // Earlier customers who fit this table keep their place in line
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT 1 FROM waiting_list
                WHERE status = 'WAITING' AND waiting_id <> ? AND num_of_customers <= ?
                LIMIT 1
            """)) {
            ps.setInt(1, waitingId);
            ps.setInt(2, tableSeats);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) { conn.rollback(sp); return null; }
            }
        }

        // Don't take a table that reservations due soon will need
        if (!RestaurantTableDAO.canSeatUpcomingReservationsFromFreeTables(conn, 120, 20)) {
            conn.rollback(sp);
            return null;
        }

        if (RestaurantTableDAO.occupyReservedTablesForWaiting(conn, waitingId) <= 0) {
            conn.rollback(sp);
            return null;
        }
        VisitDAO.insertVisit(conn, activityId, tableId);
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE waiting_list SET status = 'ARRIVED' WHERE waiting_id = ?")) {
            ps.setInt(1, waitingId);
            ps.executeUpdate();
        }
        return tableId;
    }

    // =============================================================
    // 2. TERMINAL / ASSIGNMENT LOGIC
    // =============================================================
    /**
     * Retrieves a waiting-list entry by confirmation code.
     *
     * @param code confirmation code
     * @return WaitingListDTO or null if not found
     * @throws Exception on database error
     */
    public static WaitingListDTO getByCode(String code) throws Exception {
        String sql = """
            SELECT waiting_id, num_of_customers, request_time, status, confirmation_code
            FROM waiting_list
            WHERE confirmation_code = ?
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, code);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;

                WaitingListDTO dto = new WaitingListDTO();
                dto.setId(rs.getInt("waiting_id"));
                dto.setPeopleCount(rs.getInt("num_of_customers"));
                dto.setStatus(rs.getString("status"));
                dto.setConfirmationCode(rs.getString("confirmation_code"));
                return dto;
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Cancels ASSIGNED waiting-list entries older than 15 minutes.
     * <p>
     * Also releases any tables reserved for expired assignments.
     *
     * @return number of canceled entries
     * @throws Exception on database error
     */
    public static int cancelAssignedOver15Minutes() throws Exception {
        // 1) Cancel assigned waiting_list rows older than 15 minutes
        String cancelSql = """
            UPDATE waiting_list
            SET status = 'CANCELED'
            WHERE status = 'ASSIGNED'
              AND request_time < (NOW() - INTERVAL 15 MINUTE)
        """;

        // 2) Release tables
        String releaseTablesSql = """
            UPDATE restaurant_table
            SET status = 'FREE',
                reserved_for_waiting_id = NULL,
                reserved_until = NULL
            WHERE status = 'RESERVED'
              AND reserved_for_waiting_id IS NOT NULL
              AND reserved_until IS NOT NULL
              AND reserved_until < NOW()
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);
            int canceled;
            try (PreparedStatement ps = conn.prepareStatement(cancelSql)) {
                canceled = ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(releaseTablesSql)) {
                ps.executeUpdate();
            }
            conn.commit();
            return canceled;
        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }

    
    /**
     * Cancels waiting-list entries that exceeded a given age.
     *
     * @param hours threshold in hours
     * @return number of canceled entries
     * @throws Exception on database error
     */
    public static int cancelWaitingOlderThanHours(int hours) throws Exception {
        String sql = """
            UPDATE waiting_list
            SET status = 'CANCELED'
            WHERE status = 'WAITING'
              AND request_time < (NOW() - INTERVAL ? HOUR)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, hours);
            return ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }
    
    // Used inside transactions
    public static WaitingListDTO getByCode(Connection conn, String code) throws Exception {
        String sql = """
            SELECT waiting_id, num_of_customers, request_time, status, confirmation_code
            FROM waiting_list
            WHERE confirmation_code = ?
            LIMIT 1
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, code);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;

                WaitingListDTO dto = new WaitingListDTO();
                dto.setId(rs.getInt("waiting_id"));
                dto.setPeopleCount(rs.getInt("num_of_customers"));
                dto.setStatus(rs.getString("status"));
                dto.setConfirmationCode(rs.getString("confirmation_code"));
                return dto;
            }
        }
    }

    // Used inside transactions
    public static boolean updateStatusByCode(Connection conn, String code, String newStatus) throws Exception {
        String sql = """
            UPDATE waiting_list
            SET status = ?
            WHERE confirmation_code = ?
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newStatus);
            ps.setString(2, code);
            return ps.executeUpdate() > 0;
        }
    }
    /**
     * Performs check-in for a waiting-list entry by confirmation code.
     * <p>
     * Validates status, occupies reserved tables, creates visits,
     * and updates waiting-list status to ARRIVED.
     *
     * @param code confirmation code
     * @return terminal validation response DTO
     * @throws Exception on database error
     */
    public static TerminalValidateResponseDTO checkInWaitingListByCode(String code) throws Exception {

        TerminalValidateResponseDTO dto = new TerminalValidateResponseDTO();

        if (code == null || code.isBlank()) {
            dto.setValid(false);
            dto.setMessage("Invalid confirmation code.");
            return dto;
        }

        code = code.trim();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            WaitingListDTO w = WaitingListDAO.getByCode(conn, code);
            if (w == null) {
                conn.rollback();
                dto.setValid(false);
                dto.setMessage("Code not found.");
                return dto;
            }

            String st = (w.getStatus() == null) ? "" : w.getStatus().trim();

            if (!"ASSIGNED".equalsIgnoreCase(st)) {
                conn.rollback();
                dto.setValid(true);
                dto.setStatus(st.isBlank() ? "WAITING" : st);
                dto.setNumOfCustomers(w.getPeopleCount());
                dto.setCheckInAllowed(false);
                dto.setMessage("Check-in allowed only when status is ASSIGNED.");
                dto.setTableId("-");
                return dto;
            }

            if (VisitDAO.existsVisitForWaitingId(conn, w.getId())) {
                conn.rollback();
                dto.setValid(false);
                dto.setMessage("This waiting code was already checked-in.");
                return dto;
            }

            // Multi-table support
            List<String> reservedTables = RestaurantTableDAO.getReservedTablesForWaiting(conn, w.getId());

            if (reservedTables == null || reservedTables.isEmpty()) {
                conn.rollback();
                dto.setValid(true);
                dto.setStatus("ASSIGNED");
                dto.setNumOfCustomers(w.getPeopleCount());
                dto.setCheckInAllowed(false);
                dto.setMessage("No reserved tables found (reservation may have expired). Please wait for a new assignment.");
                dto.setTableId("-");
                return dto;
            }

            int occupied = RestaurantTableDAO.occupyReservedTablesForWaiting(conn, w.getId());
            if (occupied <= 0) {
                conn.rollback();
                dto.setValid(true);
                dto.setStatus("ASSIGNED");
                dto.setNumOfCustomers(w.getPeopleCount());
                dto.setCheckInAllowed(false);
                dto.setMessage("Reserved tables expired or not available. Please wait for a new assignment.");
                dto.setTableId("-");
                return dto;
            }

            Integer activityId = null;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT activity_id
                FROM user_activity
                WHERE waiting_id = ?
                ORDER BY activity_id DESC
                LIMIT 1
            """)) {
                ps.setInt(1, w.getId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) activityId = rs.getInt(1);
                }
            }

            if (activityId == null) {
                activityId = UserActivityDAO.insertWaitingActivityReturnActivityId(
                        conn, w.getId(), null, null);
            }

            if (activityId == null || activityId <= 0) {
                conn.rollback();
                dto.setValid(false);
                dto.setMessage("Failed to create activity.");
                return dto;
            }

            for (String t : reservedTables) {
                VisitDAO.insertVisit(conn, activityId, t);
            }

            WaitingListDAO.updateStatusByCode(conn, code, "ARRIVED");

            conn.commit();

            dto.setValid(true);
            dto.setStatus("ARRIVED");
            dto.setNumOfCustomers(w.getPeopleCount());
            dto.setCheckInAllowed(false);
            dto.setMessage("Checked-in successfully (waiting list).");
            dto.setTableId(reservedTables.get(0));
            return dto;

        } catch (Exception ex) {
            try { conn.rollback(); } catch (Exception ignored) {}
            dto.setValid(false);
            dto.setMessage("Server error: " + ex.getMessage());
            return dto;
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }
    /**
     * Assigns the next suitable waiting-list entry by reserving tables.
     * <p>
     * Uses seat availability, future reservation protection,
     * and best-fit table allocation logic.
     *
     * @return assigned WaitingListDTO or null if none fit
     * @throws Exception on database error
     */
    public static WaitingListDTO assignNextWaitingByReservingTable() throws Exception {

        final int RESERVATION_LOOKAHEAD_MINUTES = 120;
        final int HOLD_MINUTES = 15;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            int freeSeatsNow = RestaurantTableDAO.getTotalSeatsAvailable(conn);
            if (freeSeatsNow <= 0) {
                conn.rollback();
                return null;
            }

            // Upcoming reservations are protected below by canSeatUpcomingReservationsFromFreeTables().
            // PENDING reservations are handled before this method by autoReserveForPendingReservations().

            String pick = """
                SELECT waiting_id, num_of_customers, confirmation_code
                FROM waiting_list
                WHERE status = 'WAITING'
                ORDER BY request_time ASC
                LIMIT 20
                FOR UPDATE
            """;

            try (PreparedStatement ps = conn.prepareStatement(pick);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {

                    int waitingId = rs.getInt("waiting_id");
                    int people = rs.getInt("num_of_customers");
                    String code = rs.getString("confirmation_code");

                    List<String> existing = RestaurantTableDAO.getReservedTablesForWaiting(conn, waitingId);
                    if (existing != null && !existing.isEmpty()) {
                        continue;
                    }

                    Savepoint sp = conn.setSavepoint("WL_TRY_" + waitingId);

                    List<String> tableIds = RestaurantTableDAO.reserveFreeTablesBestFitForWaiting(
                            conn, waitingId, people, HOLD_MINUTES);

                    if (tableIds == null || tableIds.isEmpty()) {
                        conn.rollback(sp);
                        continue;
                    }

                    boolean ok = RestaurantTableDAO.canSeatUpcomingReservationsFromFreeTables(
                            conn,
                            RESERVATION_LOOKAHEAD_MINUTES,
                            20
                    );

                    if (!ok) {
                        conn.rollback(sp);
                        continue;
                    }


                    int updated;
                    try (PreparedStatement up = conn.prepareStatement("""
                        UPDATE waiting_list
                        SET status = 'ASSIGNED',
                            request_time = NOW()
                        WHERE waiting_id = ?
                          AND status = 'WAITING'
                    """)) {
                        up.setInt(1, waitingId);
                        updated = up.executeUpdate();
                    }

                    if (updated != 1) {
                        conn.rollback(sp);
                        continue;
                    }

                    conn.commit();
                    return new WaitingListDTO(waitingId, null, null, null, people, "ASSIGNED", code);
                }
            }

            conn.rollback();
            return null;

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;

        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }
    /**
     * Cancels a waiting-list entry by confirmation code.
     * <p>
     * Releases any tables reserved for the entry.
     *
     * @param code confirmation code
     * @return true if canceled, false otherwise
     * @throws Exception on database error
     */
    public static boolean cancelAndReleaseTablesByCode(String code) throws Exception {

        if (code == null || code.isBlank()) return false;
        code = code.trim();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            WaitingListDTO w;
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT waiting_id, status
                FROM waiting_list
                WHERE confirmation_code = ?
                LIMIT 1
                FOR UPDATE
            """)) {
                ps.setString(1, code);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return false;
                    }
                    w = new WaitingListDTO();
                    w.setId(rs.getInt("waiting_id"));
                    w.setStatus(rs.getString("status"));
                }
            }

            String st = (w.getStatus() == null) ? "" : w.getStatus().trim().toUpperCase();

            if ("ARRIVED".equals(st)) {
                conn.rollback();
                return false;
            }

            int updated;
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE waiting_list
                SET status = 'CANCELED'
                WHERE waiting_id = ?
                  AND status IN ('WAITING', 'ASSIGNED')
            """)) {
                ps.setInt(1, w.getId());
                updated = ps.executeUpdate();
            }

            if (updated != 1) {
                conn.rollback();
                return false;
            }

            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE restaurant_table
                SET status = 'FREE',
                    reserved_for_waiting_id = NULL,
                    reserved_until = NULL
                WHERE status = 'RESERVED'
                  AND reserved_for_waiting_id = ?
            """)) {
                ps.setInt(1, w.getId());
                ps.executeUpdate();
            }

            conn.commit();
            return true;

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;

        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }
    
 // =============================================================
    // 3. STAFF REMOVAL METHODS (Fixes "Remove / Cancel" button)
    // =============================================================
    /**
     * Cancels a waiting-list entry by ID (staff action).
     * <p>
     * Prevents cancellation if already ARRIVED or CANCELED,
     * and releases any reserved tables.
     *
     * @param waitingId waiting-list ID
     * @return true if canceled, false otherwise
     * @throws Exception on database error
     */
    public static boolean cancelWaitingById(int waitingId) throws Exception {
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            // 1. Lock and Check Status
            String status = null;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT status FROM waiting_list WHERE waiting_id = ? FOR UPDATE")) {
                ps.setInt(1, waitingId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        status = rs.getString("status");
                    } else {
                        conn.rollback();
                        return false; // ID not found
                    }
                }
            }

            // 2. If already arrived or canceled, do not remove again
            if ("ARRIVED".equalsIgnoreCase(status) || "CANCELED".equalsIgnoreCase(status)) {
                conn.rollback();
                return false;
            }

            // 3. Update Status to CANCELED
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE waiting_list SET status = 'CANCELED' WHERE waiting_id = ?")) {
                ps.setInt(1, waitingId);
                int rows = ps.executeUpdate();
                if (rows <= 0) {
                    conn.rollback();
                    return false;
                }
            }

            // 4. Release any tables reserved for this waiting ID
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE restaurant_table
                SET status = 'FREE',
                    reserved_for_waiting_id = NULL,
                    reserved_until = NULL
                WHERE reserved_for_waiting_id = ?
            """)) {
                ps.setInt(1, waitingId);
                ps.executeUpdate();
            }

            conn.commit();
            return true;

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }
    /**
     * Retrieves contact email for a waiting-list entry.
     *
     * @param waitingId waiting-list ID
     * @return email or null if not found
     * @throws Exception on database error
     */
    public static String getEmailForWaitingId(int waitingId) throws Exception {
        String sql = """
            SELECT COALESCE(s.email, ua.guest_email) AS email
            FROM user_activity ua
            LEFT JOIN subscribers s ON s.username = ua.subscriber_username
            WHERE ua.waiting_id = ?
            ORDER BY ua.activity_id DESC
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, waitingId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("email") : null;
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Retrieves contact phone number for a waiting-list entry.
     *
     * @param waitingId waiting-list ID
     * @return phone or null if not found
     * @throws Exception on database error
     */
    public static String getPhoneForWaitingId(int waitingId) throws Exception {
        String sql = """
            SELECT COALESCE(s.phone, ua.guest_phone) AS phone
            FROM user_activity ua
            LEFT JOIN subscribers s ON s.username = ua.subscriber_username
            WHERE ua.waiting_id = ?
            ORDER BY ua.activity_id DESC
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, waitingId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("phone") : null;
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }

}