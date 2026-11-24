package DataBase.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
import DataBase.Reservation;
import Server.EmailService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import common.dto.MakeReservationRequestDTO;
import common.dto.ReservationDTO;
/**
 * Data Access Object for reservation management.
 * <p>
 * Handles the full reservation lifecycle including:
 * creation, capacity checks, arrival validation,
 * table allocation, cancellation, and expiration logic.
 * <p>
 * This class is used by both customer-facing flows
 * and terminal/staff operations.
 */
public class ReservationDAO {
	/**
	 * Inserts a new reservation record into the database.
	 *
	 * @param numOfCustomers number of guests
	 * @param reservationTime reservation start time
	 * @param expiryTime reservation expiry time
	 * @param status initial reservation status
	 * @param confirmationCode unique confirmation code
	 * @throws Exception if database operation fails
	 */
    public static void insertReservation(
            int numOfCustomers,
            Timestamp reservationTime,
            Timestamp expiryTime,
            String status,
            String confirmationCode) throws Exception {

        String sql = """
            INSERT INTO reservation
            (num_of_customers, reservation_time, expiry_time, status, confirmation_code)
            VALUES (?, ?, ?, ?, ?)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, numOfCustomers);
            ps.setTimestamp(2, reservationTime);
            ps.setTimestamp(3, expiryTime);
            ps.setString(4, status);
            ps.setString(5, confirmationCode);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }
    
    public List<Reservation> getAllReservations() throws SQLException {

        String sql = "SELECT reservation_id, num_of_customers, reservation_time, expiry_time, status, confirmation_code " +
                     "FROM reservation";

        List<Reservation> list = new ArrayList<>();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                int id = rs.getInt("reservation_id");
                int guests = rs.getInt("num_of_customers");
                Timestamp resTime = rs.getTimestamp("reservation_time");
                Timestamp expTime = rs.getTimestamp("expiry_time");
                String status = rs.getString("status");
                String code = rs.getString("confirmation_code");

                list.add(new Reservation(id, guests, resTime, expTime, status, code));
            }

        } finally {
            pool.releaseConnection(pc);
        }

        return list;
    }
    
    public static class CreateReservationResult {
        public final int reservationId;
        public final String confirmationCode;

        public CreateReservationResult(int reservationId, String confirmationCode) {
            this.reservationId = reservationId;
            this.confirmationCode = confirmationCode;
        }
    }

    /** Thrown when a reservation no longer fits at the requested time (checked atomically on insert). */
    public static class NoCapacityException extends Exception {
        private static final long serialVersionUID = 1L;
        public NoCapacityException() { super("No availability at that time (capacity reached)."); }
    }

    /**
     * Serializes "capacity check + insert" so two customers booking the last tables at the same
     * moment cannot both succeed (the check used to run outside the insert transaction).
     */
    private static final Object CREATE_LOCK = new Object();

    /**
     * Creates a reservation and its matching user_activity entry within a single transaction.
     * <p>
     * The capacity (table-packing) check runs inside the same transaction and lock as the insert,
     * and a unique confirmation code is generated.
     *
     * @param req reservation request data (subscriber or guest)
     * @return created reservation id and confirmation code
     * @throws NoCapacityException if the party no longer fits at that time
     * @throws Exception if the transaction fails
     */
    public CreateReservationResult createReservationWithActivity(MakeReservationRequestDTO req) throws Exception {

        String status = "CONFIRMED";

        Timestamp reservationTime = req.getReservationTime();

        // Dining window = 2 hours (matches the ARRIVED expiry logic)
        Timestamp expiryTime = Timestamp.valueOf(
                reservationTime.toLocalDateTime().plusMinutes(120)
        );

        String insertReservationSql = """
            INSERT INTO reservation
            (num_of_customers, reservation_time, expiry_time, status, confirmation_code)
            VALUES (?, ?, ?, ?, ?)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        synchronized (CREATE_LOCK) {
            try {
                conn.setAutoCommit(false);

                // Capacity check INSIDE the transaction and the lock (same table-packing rule as availability)
                if (!RestaurantTableDAO.canPackReservationsAtTime(conn, reservationTime, expiryTime,
                        req.getNumOfCustomers())) {
                    conn.rollback();
                    throw new NoCapacityException();
                }

                int reservationId = -1;
                String code = null;
                for (int attempt = 0; attempt < 5 && reservationId < 0; attempt++) {
                    code = UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
                    try (PreparedStatement ps = conn.prepareStatement(insertReservationSql, Statement.RETURN_GENERATED_KEYS)) {
                        ps.setInt(1, req.getNumOfCustomers());
                        ps.setTimestamp(2, reservationTime);
                        ps.setTimestamp(3, expiryTime);
                        ps.setString(4, status);
                        ps.setString(5, code);
                        ps.executeUpdate();

                        try (ResultSet rs = ps.getGeneratedKeys()) {
                            if (!rs.next()) throw new SQLException("No reservation_id generated.");
                            reservationId = rs.getInt(1);
                        }
                    } catch (SQLIntegrityConstraintViolationException duplicateCode) {
                        // extremely rare confirmation_code collision -> try another code
                    }
                }
                if (reservationId < 0) throw new SQLException("Could not generate a unique confirmation code.");

                // Insert matching row in user_activity (same transaction)
                Timestamp now = new Timestamp(System.currentTimeMillis());

                UserActivityDAO.insertActivity(
                        conn,
                        req.isSubscriber() ? req.getSubscriberUsername() : null,
                        req.isSubscriber() ? null : req.getGuestPhone(),
                        req.isSubscriber() ? null : req.getGuestEmail(),
                        reservationId,
                        null,
                        now
                );

                conn.commit();
                return new CreateReservationResult(reservationId, code);

            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignored) {}
                throw e;

            } finally {
                try { conn.setAutoCommit(true); } catch (Exception ignored) {}
                pool.releaseConnection(pc);
            }
        }
    }

    
    public static int cancelNoShows15Min() throws Exception {

        String sql = """
        	UPDATE reservation
        		SET status = 'CANCELED'
        		WHERE status = 'CONFIRMED'
        		AND reservation_time <= NOW()
        		AND TIMESTAMPDIFF(MINUTE, reservation_time, NOW()) > 15;
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }



    /**
     * Marks a reservation as ARRIVED using its confirmation code.
     * <p>
     * Validates time window, allocates or occupies tables,
     * creates visit records and updates reservation status.
     *
     * @param code reservation confirmation code
     * @return main allocated table id
     * @throws Exception if check-in is not allowed or fails
     */
    public static String markArrivedByCodeReturnTableId(String code) throws Exception {

        String sqlGet = """
            SELECT reservation_id, num_of_customers, status, reservation_time
            FROM reservation
            WHERE confirmation_code = ?
            LIMIT 1
        """;

        String sqlGetDbNow = "SELECT NOW() AS db_now";

        // ✅ allow ARRIVED from CONFIRMED OR PENDING (second scan)
        String sqlSetArrived = """
            UPDATE reservation
            SET status = 'ARRIVED',
                expiry_time = DATE_ADD(reservation_time, INTERVAL 2 HOUR)
            WHERE reservation_id = ?
              AND status IN ('CONFIRMED','PENDING')
        """;

        // ✅ cancel “late no-show” only for CONFIRMED (never for PENDING)
        String sqlCancelLate = """
            UPDATE reservation
            SET status = 'CANCELED'
            WHERE reservation_id = ?
              AND status = 'CONFIRMED'
        """;

        String sqlGetActivityId = """
            SELECT activity_id
            FROM user_activity
            WHERE reservation_id = ?
            ORDER BY activity_id DESC
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            if (code == null || code.trim().isEmpty()) {
                throw new Exception("Invalid confirmation code.");
            }

            // 1) Load reservation by code
            int reservationId;
            int numCustomers;
            String status;
            Timestamp reservationTime;

            try (PreparedStatement ps = conn.prepareStatement(sqlGet)) {
                ps.setString(1, code.trim());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new Exception("Invalid confirmation code.");
                    }
                    reservationId = rs.getInt("reservation_id");
                    numCustomers = rs.getInt("num_of_customers");
                    status = rs.getString("status");
                    reservationTime = rs.getTimestamp("reservation_time");
                }
            }

            if (!"CONFIRMED".equalsIgnoreCase(status) && !"PENDING".equalsIgnoreCase(status)) {
                throw new Exception("Check-in not allowed: status is " + status);
            }

            // Already checked-in?
            if (VisitDAO.existsVisitForReservationId(conn, reservationId)) {
                throw new Exception("Already checked-in for this reservation.");
            }

            // 2) Use DB clock to avoid timezone mismatch
            Timestamp dbNow;
            try (PreparedStatement ps = conn.prepareStatement(sqlGetDbNow);
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                dbNow = rs.getTimestamp("db_now");
            }

            if (reservationTime == null) {
                throw new Exception("Reservation time is missing.");
            }

            long nowMs = dbNow.getTime();

            // TIME WINDOW applies ONLY for CONFIRMED
            if ("CONFIRMED".equalsIgnoreCase(status)) {
                long startMs = reservationTime.getTime();
                long endMs = startMs + 15L * 60L * 1000L;

                if (nowMs < startMs) {
                    // Spec: a customer who arrives early waits and is notified when a suitable table frees.
                    // Register the arrival (PENDING); the background job holds a table and emails/SMSes them.
                    boolean changed = markPendingByCode(conn, code);
                    conn.commit();
                    throw new Exception(changed
                            ? "You are early - your arrival is registered. Please wait; we will notify you by email/SMS "
                              + "when your table is ready, then scan your code again."
                            : "Could not register your arrival. Please try again.");
                }

                if (nowMs > endMs) {
                    // cancel late no-show immediately (CONFIRMED only)
                    try (PreparedStatement ps = conn.prepareStatement(sqlCancelLate)) {
                        ps.setInt(1, reservationId);
                        ps.executeUpdate();
                    }
                    conn.commit();
                    throw new Exception("Too late. Reservation was canceled (no-show).");
                }
            }

            // 3) Find activity_id (must exist for Visit)
            Integer activityId = null;
            try (PreparedStatement ps = conn.prepareStatement(sqlGetActivityId)) {
                ps.setInt(1, reservationId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) activityId = rs.getInt("activity_id");
                }
            }

            if (activityId == null) {
                throw new Exception("Missing user_activity row for this reservation. (Fix make-reservation flow)");
            }

            // 4) Allocate/Occupy table
            String tableId = null;
            List<String> tableIds = null;

            // CONFIRMED -> allocate one FREE table (smallest that fits)
            if ("CONFIRMED".equalsIgnoreCase(status)) {

            	tableIds = RestaurantTableDAO.allocateFreeTablesBestFit(conn, reservationId, numCustomers);

             	if (tableIds == null || tableIds.isEmpty()) {
                 // Tables full -> move to PENDING (Option 1 first scan)
             		boolean changed = markPendingByCode(conn, code);
             		conn.commit(); // commit the PENDING status
             		throw new Exception(changed
                         ? "No suitable table is free yet. Please wait - we will notify you by email/SMS when your "
                           + "table is ready, then scan your code again."
                         : "No suitable table is free yet. Please try again in a few minutes.");
             }

             tableId = tableIds.get(0); // keep old behavior for returning a single "main" table
         }

            // PENDING -> second scan: must have RESERVED table for this reservation
            else { // PENDING

                tableIds = RestaurantTableDAO.getReservedTablesForReservation(conn, reservationId);

                if (tableIds == null || tableIds.isEmpty()) {
                    conn.commit();
                    throw new Exception("Still waiting for reserved tables. Please wait for the email.");
                }

                int changed = RestaurantTableDAO.occupyReservedTablesForReservation(conn, reservationId);
                if (changed <= 0) {
                    conn.commit();
                    throw new Exception("Reserved tables are not available (or 15-min window expired). Please wait for new tables.");
                }

                tableId = tableIds.get(0); // main table for UI/return
            }


            // 5) Set ARRIVED (works for CONFIRMED and PENDING now)
            int updated;
            try (PreparedStatement ps = conn.prepareStatement(sqlSetArrived)) {
                ps.setInt(1, reservationId);
                updated = ps.executeUpdate();
            }

            if (updated != 1) {
                throw new Exception("Failed to mark ARRIVED (status changed).");
            }

            // 6) Insert visit
            if (tableIds == null || tableIds.isEmpty()) {
                // fallback safety (should not happen)
                VisitDAO.insertVisit(conn, activityId, tableId);
            } else {
                for (String t : tableIds) {
                    VisitDAO.insertVisit(conn, activityId, t);
                }
            }

            conn.commit();
            return tableId;

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;

        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }





    public static common.dto.TerminalValidateResponseDTO getTerminalInfoByCode(String code) throws Exception {

        String sql = """
            SELECT
                r.reservation_id,
                r.reservation_time,
                r.num_of_customers,
                r.status,
                CASE
                  WHEN r.status = 'CONFIRMED'
                   AND NOW() >= r.reservation_time
                   AND NOW() <= DATE_ADD(r.reservation_time, INTERVAL 15 MINUTE)
                  THEN 1 ELSE 0
                END AS can_check_in,
                (NOW() < r.reservation_time) AS is_early
            FROM reservation r
            WHERE r.confirmation_code = ?
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, code == null ? "" : code.trim());

            try (ResultSet rs = ps.executeQuery()) {

                if (!rs.next()) {
                    return new common.dto.TerminalValidateResponseDTO(false, "Invalid code.");
                }

                int reservationId = rs.getInt("reservation_id");
                java.sql.Timestamp time = rs.getTimestamp("reservation_time");
                int num = rs.getInt("num_of_customers");
                String status = rs.getString("status");
                boolean canCheckIn = rs.getInt("can_check_in") == 1;

                common.dto.TerminalValidateResponseDTO dto = new common.dto.TerminalValidateResponseDTO(true, "OK");
                dto.setReservationId(reservationId);
                dto.setReservationTime(time);
                dto.setNumOfCustomers(num);
                dto.setStatus(status == null ? "" : status);

                // Default
                dto.setTableId("-");
                dto.setCheckInAllowed(false);

                // Case 1: confirmed reservation
                if ("CONFIRMED".equalsIgnoreCase(status)) {
                    boolean early = rs.getInt("is_early") == 1;
                    if (canCheckIn) {
                        dto.setCheckInAllowed(true);
                        dto.setMessage("Code valid. Ready to check-in.");
                    } else if (early) {
                        // Spec: an early customer waits and is notified when a table frees
                        dto.setCheckInAllowed(true);
                        dto.setMessage("You are early. Press Check-in to register your arrival - "
                                + "we will notify you when your table is ready.");
                    } else {
                        dto.setCheckInAllowed(false);
                        dto.setMessage("More than 15 minutes late - the reservation is canceled as a no-show.");
                    }
                    return dto;
                }

                // Case 2: waiting-for-table (PENDING)
                if ("PENDING".equalsIgnoreCase(status)) {

                    String reservedTable = RestaurantTableDAO.getReservedTableForReservation(conn, reservationId);

                    if (reservedTable != null && !reservedTable.isBlank()) {
                        dto.setTableId(reservedTable);
                        dto.setCheckInAllowed(true);
                        dto.setMessage("Table is ready: " + reservedTable + ". Press Check-in to confirm within 15 minutes.");
                    } else {
                        dto.setTableId("-");
                        dto.setCheckInAllowed(false);
                        dto.setMessage("No suitable table right now. Please wait — you will be notified when a table is ready.");
                    }
                    return dto;
                }

                // Case 3: already arrived (code is real, but nothing left to do at check-in)
                if ("ARRIVED".equalsIgnoreCase(status)) {
                    dto.setCheckInAllowed(false);
                    dto.setMessage("Already arrived.");
                    return dto;
                }

                // CANCELED / EXPIRED / anything else: the code exists but is no longer usable.
                // (It used to be reported as valid, so the terminal showed "VALID" for canceled reservations.)
                dto.setValid(false);
                dto.setCheckInAllowed(false);
                dto.setMessage("CANCELED".equalsIgnoreCase(status)
                        ? "This reservation was canceled."
                        : "EXPIRED".equalsIgnoreCase(status)
                            ? "This reservation is already finished."
                            : "Reservation status: " + status);
                return dto;
            }

        } finally {
            pool.releaseConnection(pc);
        }
    }



    /**
     * True if a party of {@code numCustomers} can be seated for 2 hours starting at {@code start},
     * together with the reservations that are present at the same time (table-packing rule).
     */
    public static boolean canFitAtTime(Timestamp start, int numCustomers) throws Exception {
        Timestamp end = Timestamp.valueOf(start.toLocalDateTime().plusMinutes(120));
        return RestaurantTableDAO.canPackReservationsAtTime(start, end, numCustomers);
    }


    public static String getReservationEmail(int reservationId) throws Exception {
        String sql = """
            SELECT COALESCE(s.email, ua.guest_email) AS email
            FROM user_activity ua
            LEFT JOIN subscribers s ON s.username = ua.subscriber_username
            WHERE ua.reservation_id = ?
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String email = rs.getString("email");
                    return (email == null || email.isBlank()) ? null : email.trim();
                }
                return null;
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }

    public static String getReservationPhone(int reservationId) throws Exception {
        String sql = """
            SELECT COALESCE(s.phone, ua.guest_phone) AS phone
            FROM user_activity ua
            LEFT JOIN subscribers s ON s.username = ua.subscriber_username
            WHERE ua.reservation_id = ?
            LIMIT 1
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String phone = rs.getString("phone");
                    return (phone == null || phone.isBlank()) ? null : phone.trim();
                }
                return null;
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }

    public List<Reservation> getReservationsBySubscriber(String username) throws Exception {

        String sql = """
            SELECT r.reservation_id, r.num_of_customers, r.reservation_time,
                   r.expiry_time, r.status, r.confirmation_code
            FROM reservation r
            JOIN user_activity ua ON ua.reservation_id = r.reservation_id
            WHERE ua.subscriber_username = ?
            ORDER BY r.reservation_time DESC
        """;

        List<Reservation> list = new ArrayList<>();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                list.add(new Reservation(
                    rs.getInt("reservation_id"),
                    rs.getInt("num_of_customers"),
                    rs.getTimestamp("reservation_time"),
                    rs.getTimestamp("expiry_time"),
                    rs.getString("status"),
                    rs.getString("confirmation_code")
                ));
            }
        } finally {
            pool.releaseConnection(pc);
        }

        return list;
    }

    /**
     * Guest identification (spec: "phone and/or email"):
     * at least one value the guest typed must match the reservation, and no typed value may contradict
     * the stored one (a contact that was never given, i.e. NULL, is not compared).
     * Has 8 placeholders, bound by {@link #bindGuestMatch}.
     */
    private static final String GUEST_MATCH_SQL = """
              AND ( (? <> '' AND ua.guest_email = ?) OR (? <> '' AND ua.guest_phone = ?) )
              AND ( ? = '' OR ua.guest_email IS NULL OR ua.guest_email = ? )
              AND ( ? = '' OR ua.guest_phone IS NULL OR ua.guest_phone = ? )
            """;

    /** Binds the 8 placeholders of {@link #GUEST_MATCH_SQL} starting at {@code first}. */
    private static void bindGuestMatch(PreparedStatement ps, int first, String email, String phone) throws SQLException {
        String e = email == null ? "" : email.trim();
        String p = phone == null ? "" : phone.trim();
        String[] values = { e, e, p, p, e, e, p, p };
        for (int i = 0; i < values.length; i++) ps.setString(first + i, values[i]);
    }

    public List<Reservation> getReservationsByGuest(String email, String phone) throws Exception {

        String sql = """
            SELECT r.reservation_id, r.num_of_customers, r.reservation_time,
                   r.expiry_time, r.status, r.confirmation_code
            FROM reservation r
            JOIN user_activity ua ON ua.reservation_id = r.reservation_id
            WHERE ua.subscriber_username IS NULL
        """ + GUEST_MATCH_SQL + """
            ORDER BY r.reservation_time DESC
        """;

        List<Reservation> list = new ArrayList<>();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindGuestMatch(ps, 1, email, phone);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(new Reservation(
                    rs.getInt("reservation_id"),
                    rs.getInt("num_of_customers"),
                    rs.getTimestamp("reservation_time"),
                    rs.getTimestamp("expiry_time"),
                    rs.getString("status"),
                    rs.getString("confirmation_code")
                ));
            }
        } finally {
            pool.releaseConnection(pc);
        }

        return list;
    }
    
    /**
     * Cancel a reservation (set status to CANCELLED) only if:
     * - it belongs to the requester (subscriber_username OR guest_email+guest_phone in user_activity)
     * - and reservation is still cancellable (currently CONFIRMED)
     *
     * @return true if the reservation was updated, false otherwise
     */
    public static boolean cancelReservation(int reservationId, String role, String username, String guestEmail, String guestPhone) throws Exception {
        if (role == null) return false;
        String r = role.trim().toUpperCase();

        if ("SUBSCRIBER".equals(r)) {
            return cancelReservationForSubscriber(reservationId, username);
        } else {
            // treat anything else as CUSTOMER/GUEST
            return cancelReservationForGuest(reservationId, guestEmail, guestPhone);
        }
    }

    private static boolean cancelReservationForSubscriber(int reservationId, String username) throws Exception {
        if (reservationId <= 0) return false;
        if (username == null || username.isBlank()) return false;

        String sql = """
            UPDATE reservation r
            JOIN user_activity ua ON ua.reservation_id = r.reservation_id
            SET r.status = 'CANCELED'
            WHERE r.reservation_id = ?
              AND r.status = 'CONFIRMED'
              AND ua.subscriber_username = ?
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);
            ps.setString(2, username);
            int updated = ps.executeUpdate();
            return updated > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }

    private static boolean cancelReservationForGuest(int reservationId, String email, String phone) throws Exception {
        if (reservationId <= 0) return false;
        boolean noEmail = email == null || email.isBlank();
        boolean noPhone = phone == null || phone.isBlank();
        if (noEmail && noPhone) return false;

        // Same identity rule as getReservationsByGuest(): phone and/or email (spec)
        String sql = """
            UPDATE reservation r
            JOIN user_activity ua ON ua.reservation_id = r.reservation_id
            SET r.status = 'CANCELED'
            WHERE r.reservation_id = ?
              AND r.status = 'CONFIRMED'
              AND ua.subscriber_username IS NULL
        """ + GUEST_MATCH_SQL;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);
            bindGuestMatch(ps, 2, email, phone);
            int updated = ps.executeUpdate();
            return updated > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }


    public static boolean markPendingByCode(Connection conn, String code) throws Exception {
        String sql = """
            UPDATE reservation
            SET status = 'PENDING'
            WHERE confirmation_code = ?
              AND status = 'CONFIRMED'
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, code == null ? "" : code.trim());
            return ps.executeUpdate() > 0;
        }
    }



    public static void autoReserveForPendingReservations() throws Exception {

        // early = the customer arrived before the reservation time (PENDING, reservation_time in the future)
        String pickSql = """
            SELECT reservation_id, num_of_customers, (reservation_time > NOW()) AS early
            FROM reservation
            WHERE status = 'PENDING'
            ORDER BY reservation_time ASC
            LIMIT 20
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            List<int[]> candidates = new ArrayList<>();

            try (PreparedStatement ps = conn.prepareStatement(pickSql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    candidates.add(new int[]{
                            rs.getInt("reservation_id"),
                            rs.getInt("num_of_customers"),
                            rs.getInt("early")
                    });
                }
            }

            for (int[] c : candidates) {
                int resId = c[0];
                int seats = c[1];
                boolean early = c[2] == 1;

                // already holding a table?
                List<String> existing =
                        RestaurantTableDAO.getReservedTablesForReservation(conn, resId);
                if (existing != null && !existing.isEmpty()) {
                    continue;
                }

                java.sql.Savepoint sp = conn.setSavepoint();
                List<String> tableIds =
                        RestaurantTableDAO.reserveFreeTablesBestFitForReservation(
                                conn, resId, seats, 15);

                if (tableIds == null || tableIds.isEmpty()) {
                    conn.rollback(sp);
                    continue;
                }

                // An EARLY customer may only take a table that isn't needed by reservations due soon
                if (early && !RestaurantTableDAO.canSeatUpcomingReservationsFromFreeTables(conn, 120, 20)) {
                    conn.rollback(sp);
                    continue;
                }

                conn.commit();

                // Spec: notify by email AND SMS when a suitable table frees
                try {
                    String email = getReservationEmail(resId);
                    if (email != null && !email.isBlank()) {
                        EmailService.sendReservationTableReady(email, String.join(",", tableIds));
                    }
                    EmailService.smsStub(getReservationPhone(resId),
                            "Your table " + String.join(",", tableIds) + " is ready. Scan your code within 15 minutes.");
                } catch (Exception ignored) {}
            }

            conn.rollback(); // release anything not committed

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;

        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }

    public static int cancelPendingReservationsWithExpiredHold() throws Exception {

        String sql = """
            UPDATE reservation r
            JOIN restaurant_table t
              ON t.reserved_for_reservation_id = r.reservation_id
            SET r.status = 'CANCELED',
                t.status = 'FREE',
                t.reserved_for_reservation_id = NULL,
                t.reserved_until = NULL
            WHERE r.status = 'PENDING'
              AND t.status = 'RESERVED'
              AND t.reserved_until < NOW()
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }




    
    public static class CancelByCodeResult {
        public final boolean ok;
        public final String message;

        public CancelByCodeResult(boolean ok, String message) {
            this.ok = ok;
            this.message = message;
        }
    }

    /**
     * Cancels a reservation by confirmation code.
     * <p>
     * Allowed only for CONFIRMED or PENDING reservations.
     * Also releases any reserved tables.
     *
     * @param confirmationCode reservation confirmation code
     * @return result object with status and message
     * @throws Exception on database failure
     */
    public static CancelByCodeResult cancelReservationByCode(String confirmationCode) throws Exception {
        if (confirmationCode == null || confirmationCode.trim().isEmpty()) {
            return new CancelByCodeResult(false, "Missing confirmation code.");
        }

        String code = confirmationCode.trim();

        String sqlGet = """
            SELECT reservation_id, status
            FROM reservation
            WHERE confirmation_code = ?
            LIMIT 1
            FOR UPDATE
        """;

        String sqlCancel = """
            UPDATE reservation
            SET status = 'CANCELED'
            WHERE reservation_id = ?
              AND status IN ('CONFIRMED','PENDING')
        """;

        String sqlReleaseTables = """
            UPDATE restaurant_table
            SET status = 'FREE',
                reserved_for_reservation_id = NULL,
                reserved_until = NULL
            WHERE reserved_for_reservation_id = ?
              AND status = 'RESERVED'
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            Integer resId = null;
            String status = null;

            try (PreparedStatement ps = conn.prepareStatement(sqlGet)) {
                ps.setString(1, code);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return new CancelByCodeResult(false, "Code not found.");
                    }
                    resId = rs.getInt("reservation_id");
                    status = rs.getString("status");
                }
            }

            if (status == null) status = "";

            // Block if already arrived / not cancelable
            if (!status.equalsIgnoreCase("CONFIRMED") && !status.equalsIgnoreCase("PENDING")) {
                conn.rollback();
                return new CancelByCodeResult(false, "Cannot cancel. Status is: " + status);
            }

            // If there is a visit already -> don’t allow cancel (extra safety)
            if (VisitDAO.existsVisitForReservationId(conn, resId)) {
                conn.rollback();
                return new CancelByCodeResult(false, "Already checked-in. Cannot cancel now.");
            }

            int updated;
            try (PreparedStatement ps = conn.prepareStatement(sqlCancel)) {
                ps.setInt(1, resId);
                updated = ps.executeUpdate();
            }

            if (updated != 1) {
                conn.rollback();
                return new CancelByCodeResult(false, "Cancel failed (status changed).");
            }

            // Release reserved tables (important for PENDING)
            try (PreparedStatement ps = conn.prepareStatement(sqlReleaseTables)) {
                ps.setInt(1, resId);
                ps.executeUpdate();
            }

            conn.commit();
            return new CancelByCodeResult(true, "✅ Reservation canceled.");

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;

        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }
    
    public static List<ReservationDTO> getReservationsBySubscriberForStaff(String username) {
        List<ReservationDTO> list = new ArrayList<>();
        // ✅ JOIN with user_activity to find the username
        String sql = "SELECT r.* FROM reservation r " +
                     "JOIN user_activity ua ON r.reservation_id = ua.reservation_id " +
                     "WHERE ua.subscriber_username = ?";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new ReservationDTO(
                        rs.getInt("reservation_id"), 
                        rs.getString("confirmation_code"),
                        rs.getString("reservation_time"),
                        rs.getString("expiry_time"),
                        rs.getInt("num_of_customers"),
                        rs.getString("status")
                    ));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            pool.releaseConnection(pc);
        }
        return list;
    }

    /**
     * Party sizes of CONFIRMED reservations starting within the look-ahead window.
     * ARRIVED reservations are not included: they already sit at OCCUPIED tables,
     * counting them again would block the waiting list for no reason.
     */
    public static List<Integer> getUpcomingReservationSizes(Connection conn, int lookAheadMinutes, int limit) throws Exception {
        List<Integer> needs = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT num_of_customers
            FROM reservation
            WHERE status = 'CONFIRMED'
              AND reservation_time BETWEEN NOW() AND (NOW() + INTERVAL ? MINUTE)
            ORDER BY reservation_time ASC
            LIMIT ?
        """)) {
            ps.setInt(1, lookAheadMinutes);
            ps.setInt(2, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) needs.add(rs.getInt(1));
            }
        }

        return needs;
    }
    
    
    /**
     * Identifies and cancels existing reservations that conflict with updated opening hours.
     * <p>
     * This method queries for "CONFIRMED" reservations that are now invalid because:
     * </p>
     * <ul>
     * <li>They fall on the specific date (if provided) or day of the week.</li>
     * <li>Their time is before the new opening time.</li>
     * <li>Their time is too close to the new closing time (less than 2 hours before close).</li>
     * </ul>
     * Any matching reservations are updated to "CANCELED".
     * 
     *
     * @param dayOfWeek the day of the week to check (e.g., "Monday")
     * @param specialDate a specific date string (YYYY-MM-DD) to check, or null for recurring weekly hours
     * @param newOpen the new opening time string (HH:mm)
     * @param newClose the new closing time string (HH:mm)
     * @return a list of string arrays containing contact info for affected customers: 
     * {@code [username, guestEmail, guestPhone]}
     * @throws Exception if a database access error occurs during query or update
     */
    public static List<String[]> cancelConflictsAfterHoursUpdate(String dayOfWeek, String specialDate, String newOpen, String newClose) throws Exception {
        List<String[]> affectedContacts = new ArrayList<>();
        
        // 1. Identify conflicting reservations
        // logic: (Too Early) OR (Too Late for 2-hour meal)
        // AND matches the specific Date OR the Day of Week
        String selectSql = """
            SELECT r.reservation_id, ua.subscriber_username, ua.guest_email, ua.guest_phone
            FROM reservation r
            JOIN user_activity ua ON r.reservation_id = ua.reservation_id
            WHERE r.status = 'CONFIRMED'
              AND r.reservation_time > NOW()
              AND (
                  ( ? IS NOT NULL AND DATE(r.reservation_time) = ? )
                  OR
                  ( ? IS NULL AND DAYNAME(r.reservation_time) = ?
                    -- weekly change: dates that have their own special hours are not affected
                    AND NOT EXISTS (SELECT 1 FROM opening_hours oh
                                    WHERE oh.is_special = 'YES'
                                      AND oh.special_date = DATE(r.reservation_time)) )
              )
              AND (
                  TIME(r.reservation_time) < CAST(? AS TIME)
                  OR 
                  TIME(r.reservation_time) > SUBTIME(CAST(? AS TIME), '02:00:00')
              )
        """;

        String updateSql = """
            UPDATE reservation r
            SET r.status = 'CANCELED'
            WHERE r.reservation_id = ?
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            List<Integer> idsToCancel = new ArrayList<>();

            // A. Find Conflicts
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                // Special Date Param (twice)
                ps.setString(1, specialDate);
                ps.setString(2, specialDate);
                
                // Weekly Param (twice)
                // Note: SQL DAYNAME returns "Sunday", "Monday", etc. Ensure inputs match.
                ps.setString(3, specialDate); // If specialDate is not null, this part is ignored by OR logic anyway
                ps.setString(4, dayOfWeek); 
                
                // Time Constraints
                ps.setString(5, newOpen);
                ps.setString(6, newClose);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        idsToCancel.add(rs.getInt("reservation_id"));
                        
                        // Collect contact info for Email/SMS
                        String user = rs.getString("subscriber_username");
                        String mail = rs.getString("guest_email");
                        String phone = rs.getString("guest_phone");
                        
                        // If subscriber, we might need to fetch email separately if not in user_activity (depending on your schema)
                        // But for now assuming simple collection:
                        affectedContacts.add(new String[]{ user, mail, phone });
                    }
                }
            }

            // B. Cancel Them
            if (!idsToCancel.isEmpty()) {
                try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                    for (int id : idsToCancel) {
                        ps.setInt(1, id);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }

            conn.commit();
            return affectedContacts;

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }

    /**
     * CONFIRMED reservations whose start time is in (from, to]. Used by the 2-hour reminder job.
     */
    public static List<Reservation> getConfirmedReservationsStartingBetween(Timestamp from, Timestamp to) throws Exception {
        String sql = """
            SELECT reservation_id, num_of_customers, reservation_time, expiry_time, status, confirmation_code
            FROM reservation
            WHERE status = 'CONFIRMED'
              AND reservation_time > ?
              AND reservation_time <= ?
        """;

        List<Reservation> list = new ArrayList<>();
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, from);
            ps.setTimestamp(2, to);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new Reservation(
                            rs.getInt("reservation_id"),
                            rs.getInt("num_of_customers"),
                            rs.getTimestamp("reservation_time"),
                            rs.getTimestamp("expiry_time"),
                            rs.getString("status"),
                            rs.getString("confirmation_code")));
                }
            }
        } finally {
            pool.releaseConnection(pc);
        }
        return list;
    }

    public static int countReservationsMadeToday() throws Exception {
        String sql = """
			SELECT COUNT(*)
			FROM reservation
			WHERE DATE(reservation_time) = CURDATE()
			  AND status IN ('PENDING','CONFIRMED','ARRIVED');
        """;

        var pool = MySQLConnectionPool.getInstance();
        var pc = pool.getConnection();
        var conn = pc.getConnection();

        try (var ps = conn.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        } finally {
            pool.releaseConnection(pc);
        }
    }


}