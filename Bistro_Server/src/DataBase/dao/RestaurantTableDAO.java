package DataBase.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
import common.dto.RestaurantTableDTO;
/**
 * Data Access Object for restaurant table management.
 * <p>
 * Responsible for table inventory operations including:
 * allocation, reservation (hold), occupation, release,
 * and feasibility planning for seating reservations.
 * <p>
 * Each party gets ONE whole table (tables are never combined or split, as the spec says),
 * reservation waiting logic, and planning simulations
 * without modifying table status.
 */
public class RestaurantTableDAO {
	/**
	 * Inserts or updates a restaurant table definition.
	 * <p>
	 * If the table already exists, only the seat count is updated.
	 *
	 * @param tableId unique table identifier
	 * @param seats number of seats at the table
	 * @param status initial table status (FREE / RESERVED / OCCUPIED)
	 * @throws Exception on database error
	 */
	public static void insertTable(String tableId, int seats, String status) throws Exception {
        String sql = """
            INSERT INTO restaurant_table (table_id, num_of_seats, status)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE num_of_seats = VALUES(num_of_seats)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setInt(2, seats);
            ps.setString(3, status);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Calculates the total seating capacity of the restaurant.
     *
     * @return total number of seats across all tables
     * @throws Exception on query failure
     */
    public static int getTotalSeats() throws Exception {
        String sql =
                "SELECT COALESCE(SUM(num_of_seats),0) AS total " +
                "FROM restaurant_table";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            return rs.next() ? rs.getInt("total") : 0;

        } finally {
            pool.releaseConnection(pc);
        }
    }

    /**
     * Allocates a single FREE table that can fit the given number of customers.
     * <p>
     * Chooses the smallest suitable table (best-fit) and marks it OCCUPIED.
     *
     * @param conn active database connection
     * @param numCustomers party size
     * @return allocated table id, or null if none available
     * @throws Exception on database error
     */
    public static String allocateFreeTable(Connection conn, int numCustomers) throws Exception {
        String selectSql = """
            SELECT table_id
            FROM restaurant_table
            WHERE status = 'FREE' AND num_of_seats >= ?
            ORDER BY num_of_seats ASC
            LIMIT 1
        """;

        String updateSql = """
            UPDATE restaurant_table
            SET status = 'OCCUPIED'
            WHERE table_id = ? AND status = 'FREE'
        """;

        String tableId = null;

        try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
            ps.setInt(1, numCustomers);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) tableId = rs.getString("table_id");
            }
        }

        if (tableId == null) return null;

        try (PreparedStatement ps2 = conn.prepareStatement(updateSql)) {
            ps2.setString(1, tableId);
            int updated = ps2.executeUpdate();
            if (updated != 1) return null; // someone took it
        }

        return tableId;
    }
    
 // === Option-1: REAL reservation hold using restaurant_table columns ===
    /**
     * Reserves a FREE table for a reservation for a limited hold time.
     * <p>
     * Marks the table as RESERVED and links it to the reservation id.
     *
     * @param conn active database connection
     * @param numCustomers party size
     * @param reservationId reservation identifier
     * @param holdMinutes reservation hold duration
     * @return reserved table id, or null if none available
     * @throws Exception on database error
     */
    public static String reserveFreeTableForReservation(Connection conn, int numCustomers, int reservationId, int holdMinutes) throws Exception {

        // pick smallest FREE that fits
        String selectSql = """
            SELECT table_id
            FROM restaurant_table
            WHERE status = 'FREE' AND num_of_seats >= ?
            ORDER BY num_of_seats ASC
            LIMIT 1
        """;

        // reserve it for THIS reservation (atomic)
        String updateSql = """
            UPDATE restaurant_table
            SET status = 'RESERVED',
                reserved_for_reservation_id = ?,
                reserved_until = DATE_ADD(NOW(), INTERVAL ? MINUTE)
            WHERE table_id = ?
              AND status = 'FREE'
        """;

        String tableId = null;

        try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
            ps.setInt(1, numCustomers);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) tableId = rs.getString("table_id");
            }
        }

        if (tableId == null) return null;

        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            ps.setInt(1, reservationId);
            ps.setInt(2, holdMinutes);
            ps.setString(3, tableId);

            int updated = ps.executeUpdate();
            if (updated == 0) return null; // race: someone took it
        }

        return tableId;
    }

    public static String getReservedTableForReservation(Connection conn, int reservationId) throws Exception {
        String sql = """
            SELECT table_id
            FROM restaurant_table
            WHERE status = 'RESERVED'
              AND reserved_for_reservation_id = ?
              AND reserved_until > NOW()
            LIMIT 1
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("table_id");
                return null;
            }
        }
    }




    
    public static String reserveFreeTableForWaitingReturnTableId(Connection conn, int waitingId, int people) throws Exception {
        // 1) pick best-fit FREE table that is not already reserved
        String select = """
            SELECT table_id
            FROM restaurant_table
            WHERE status = 'FREE'
              AND num_of_seats >= ?
              AND reserved_for_reservation_id IS NULL
              AND reserved_for_waiting_id IS NULL
            ORDER BY num_of_seats ASC
            LIMIT 1
            FOR UPDATE
        """;

        String update = """
            UPDATE restaurant_table
            SET status = 'RESERVED',
                reserved_for_waiting_id = ?,
                reserved_until = DATE_ADD(NOW(), INTERVAL 15 MINUTE)
            WHERE table_id = ?
              AND status = 'FREE'
              AND reserved_for_reservation_id IS NULL
              AND reserved_for_waiting_id IS NULL
        """;

        String tableId = null;

        try (PreparedStatement ps = conn.prepareStatement(select)) {
            ps.setInt(1, people);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) tableId = rs.getString("table_id");
            }
        }

        if (tableId == null) return null;

        try (PreparedStatement ps = conn.prepareStatement(update)) {
            ps.setInt(1, waitingId);
            ps.setString(2, tableId);
            int updated = ps.executeUpdate();
            return (updated == 1) ? tableId : null;
        }
    }

    public static int releaseExpiredReservedTablesForWaiting() throws Exception {
        String sql = """
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

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }

    private static class TableCandidate {
        final String tableId;
        final int seats;
        TableCandidate(String tableId, int seats) {
            this.tableId = tableId;
            this.seats = seats;
        }
    }

    /**
     * Picks ONE table for a party: the smallest table with enough seats (least wasted seats).
     * <p>
     * Spec: "no need to combine/split tables - each reservation occupies one whole table",
     * so a party is never spread over several tables.
     *
     * @return the chosen table, or null if no single table is big enough
     */
    private static TableCandidate chooseTable(List<TableCandidate> tables, int needed) {
        TableCandidate best = null;
        for (TableCandidate t : tables) {
            if (t.seats >= needed && (best == null || t.seats < best.seats)) best = t;
        }
        return best;
    }

    /**
     * Size of the biggest table (0 if there are no tables). A party larger than this can never be seated.
     */
    public static int getMaxTableSeats() throws Exception {
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COALESCE(MAX(num_of_seats),0) FROM restaurant_table");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }

    /**
     * Allocates ONE free table (smallest that fits) for a reservation at check-in time
     * and links it to the reservation.
     *
     * @param conn active database connection
     * @param reservationId reservation identifier
     * @param numCustomers party size
     * @return list with the allocated table id, or null if no single free table fits
     * @throws Exception if allocation fails (atomic operation)
     */
    public static List<String> allocateFreeTablesBestFit(Connection conn, int reservationId, int numCustomers) throws Exception {
        String single = allocateFreeTable(conn, numCustomers);
        if (single != null) {
            // store linkage so we can free all later by reservationId
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE restaurant_table
                SET reserved_for_reservation_id = ?
                WHERE table_id = ?
            """)) {
                ps.setInt(1, reservationId);
                ps.setString(2, single);
                int updated = ps.executeUpdate();
                if (updated != 1) {
                    // This should not happen, but if it does we must fail to force rollback
                    throw new Exception("Failed to link occupied table to reservation: " + single);
                }
            }
            List<String> list = new ArrayList<>();
            list.add(single);
            return list;
        }
        return null; // no single free table is big enough (tables are never combined)
    }

    /**
     * Holds ONE free table (smallest that fits) for a PENDING reservation for a limited time.
     *
     * @param conn active database connection
     * @param reservationId reservation identifier
     * @param numCustomers party size
     * @param holdMinutes reservation hold duration
     * @return list with the reserved table id, or null if no single free table fits
     * @throws Exception on database error
     */
    public static List<String> reserveFreeTablesBestFitForReservation(Connection conn, int reservationId, int numCustomers, int holdMinutes) throws Exception {
        String one = reserveFreeTableForReservation(conn, numCustomers, reservationId, holdMinutes);
        if (one == null) return null;
        List<String> list = new ArrayList<>();
        list.add(one);
        return list;
    }

    public static List<String> getReservedTablesForReservation(Connection conn, int reservationId) throws Exception {
        String sql = """
            SELECT table_id
            FROM restaurant_table
            WHERE status = 'RESERVED'
              AND reserved_for_reservation_id = ?
              AND reserved_until > NOW()
            ORDER BY num_of_seats DESC
        """;
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, reservationId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }
    /**
     * Converts all RESERVED tables of a reservation into OCCUPIED.
     * Used when a customer checks in.
     *
     * @param conn active database connection
     * @param reservationId reservation identifier
     * @return number of tables updated
     * @throws Exception on database error
     */
    public static int occupyReservedTablesForReservation(Connection conn, int reservationId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            UPDATE restaurant_table
            SET status = 'OCCUPIED',
                reserved_until = NULL
            WHERE reserved_for_reservation_id = ?
              AND status = 'RESERVED'
        """)) {
            ps.setInt(1, reservationId);
            return ps.executeUpdate();
        }
    }
    /**
     * Frees all OCCUPIED tables linked to a reservation.
     * Used after payment / checkout.
     *
     * @param conn active database connection
     * @param reservationId reservation identifier
     * @return number of tables released
     * @throws Exception on database error
     */
    public static int freeOccupiedTablesForReservation(Connection conn, int reservationId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            UPDATE restaurant_table
            SET status = 'FREE',
                reserved_for_reservation_id = NULL,
                reserved_until = NULL
            WHERE reserved_for_reservation_id = ?
              AND status = 'OCCUPIED'
        """)) {
            ps.setInt(1, reservationId);
            return ps.executeUpdate();
        }
    }
    
 // ==============================
 // Waiting List (one whole table per party)
 // ==============================

 /**
  * Holds ONE free table (smallest that fits) for a waiting-list entry for 15 minutes.
  *
  * @return list with the reserved table id, or null if no single free table fits
  */
 public static List<String> reserveFreeTablesBestFitForWaiting(Connection conn, int waitingId, int people, int holdMinutes) throws Exception {
     String one = reserveFreeTableForWaitingReturnTableId(conn, waitingId, people);
     if (one == null) return null;
     List<String> list = new ArrayList<>();
     list.add(one);
     return list;
 }

 /**
  * Get ALL reserved tables for waitingId (still within reserved_until window).
  */
 public static List<String> getReservedTablesForWaiting(Connection conn, int waitingId) throws Exception {
     String sql = """
         SELECT table_id
         FROM restaurant_table
         WHERE status = 'RESERVED'
           AND reserved_for_waiting_id = ?
           AND reserved_until IS NOT NULL
           AND reserved_until >= NOW()
         ORDER BY num_of_seats DESC
     """;

     List<String> out = new ArrayList<>();
     try (PreparedStatement ps = conn.prepareStatement(sql)) {
         ps.setInt(1, waitingId);
         try (ResultSet rs = ps.executeQuery()) {
             while (rs.next()) out.add(rs.getString("table_id"));
         }
     }
     return out;
 }

 /**
  * Occupy ALL reserved tables for waitingId (customer arrived / checked-in).
  * NOTE: we KEEP reserved_for_waiting_id so we can free all tables on pay.
  */
 public static int occupyReservedTablesForWaiting(Connection conn, int waitingId) throws Exception {
     String sql = """
         UPDATE restaurant_table
         SET status = 'OCCUPIED',
             reserved_until = NULL
         WHERE status = 'RESERVED'
           AND reserved_for_waiting_id = ?
           AND reserved_until IS NOT NULL
           AND reserved_until >= NOW()
     """;

     try (PreparedStatement ps = conn.prepareStatement(sql)) {
         ps.setInt(1, waitingId);
         return ps.executeUpdate();
     }
 }

 /**
  * Free ALL occupied tables for waitingId (after payment).
  */
 public static int freeOccupiedTablesForWaiting(Connection conn, int waitingId) throws Exception {
     String sql = """
         UPDATE restaurant_table
         SET status = 'FREE',
             reserved_for_waiting_id = NULL,
             reserved_until = NULL
         WHERE status = 'OCCUPIED'
           AND reserved_for_waiting_id = ?
     """;

     try (PreparedStatement ps = conn.prepareStatement(sql)) {
         ps.setInt(1, waitingId);
         return ps.executeUpdate();
     }
 }
 
 /**
  * Calculates the total number of seats currently available in the restaurant.
  * <p>
  * This method queries the sum of seats for all tables that currently have the status 'FREE'.
  * It uses the provided connection to execute the query.
  * </p>
  *
  * @param conn the active database connection
  * @return the total count of free seats
  * @throws Exception if a database access error occurs
  */
 public static int getTotalSeatsAvailable(Connection conn) throws Exception {
	    String sql = """
	        SELECT COALESCE(SUM(num_of_seats),0) AS total
	        FROM restaurant_table
	        WHERE status = 'FREE'
	    """;

	    try (PreparedStatement ps = conn.prepareStatement(sql);
	         ResultSet rs = ps.executeQuery()) {
	        return rs.next() ? rs.getInt("total") : 0;
	    }
	}

	
	
	/**
	 * Retrieves a complete list of all restaurant tables from the database.
	 * <p>
	 * Returns a list of {@link RestaurantTableDTO} objects, including their ID,
	 * seat count, and current status (FREE, OCCUPIED, RESERVED).
	 * The list is sorted by {@code table_id}.
	 * </p>
	 *
	 * @return a list of all restaurant tables
	 * @throws Exception if a database access error occurs
	 */
	public static List<RestaurantTableDTO> getAllTables() throws Exception {
        List<RestaurantTableDTO> list = new ArrayList<>();
        String sql = "SELECT * FROM restaurant_table ORDER BY table_id";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                list.add(new RestaurantTableDTO(
                    rs.getString("table_id"),
                    rs.getInt("num_of_seats"),
                    rs.getString("status")
                ));
            }
        } finally {
            pool.releaseConnection(pc);
        }
        return list;
    }
	
	/**
	 * Permanently deletes a table from the database by its ID.
	 * <p>
	 * <b>Note:</b> This method performs the raw SQL deletion. It does not check
	 * if the table is currently occupied or needed for future reservations.
	 * The {@link #isSafeToDelete(String)} method should usually be called before this.
	 * </p>
	 *
	 * @param tableId the ID of the table to delete
	 * @return {@code true} if the table was found and deleted; {@code false} otherwise
	 * @throws Exception if a database access error occurs
	 */
	public static boolean deleteTable(String tableId) throws Exception {
        String sql = "DELETE FROM restaurant_table WHERE table_id = ?";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableId);
            return ps.executeUpdate() > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }
	
	/**
	 * Updates the seating capacity of an existing table.
	 * <p>
	 * Modifies the {@code num_of_seats} column for the specified table ID.
	 * </p>
	 *
	 * @param tableId the ID of the table to update
	 * @param newSeats the new number of seats
	 * @return {@code true} if the table was found and updated; {@code false} otherwise
	 * @throws Exception if a database access error occurs
	 */
	public static boolean updateTableSeats(String tableId, int newSeats) throws Exception {
        String sql = "UPDATE restaurant_table SET num_of_seats = ? WHERE table_id = ?";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, newSeats);
            ps.setString(2, tableId);
            return ps.executeUpdate() > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }

	// ====== Policy 1: Table-feasibility planning (NO status changes) ======

	/**
	 * Load ALL restaurant tables (capacity list), regardless of status.
	 * Planning assumes the table inventory is the "container set".
	 */
	private static List<TableCandidate> getAllTablesForPlanning(Connection conn) throws Exception {
	    String sql = """
	        SELECT table_id, num_of_seats
	        FROM restaurant_table
	        ORDER BY num_of_seats DESC
	    """;
	    List<TableCandidate> out = new ArrayList<>();
	    try (PreparedStatement ps = conn.prepareStatement(sql);
	         ResultSet rs = ps.executeQuery()) {
	        while (rs.next()) {
	            out.add(new TableCandidate(rs.getString("table_id"), rs.getInt("num_of_seats")));
	        }
	    }
	    return out;
	}

	/**
	 * Loads reservations that overlap the window [start, end) and need a table:
	 * CONFIRMED (coming), PENDING (arrived, waiting for a table) and ARRIVED (seated).
	 *
	 * @return list of {startMillis, endMillis, partySize}
	 */
	private static List<long[]> getOverlappingReservations(Connection conn,
	                                                       java.sql.Timestamp start,
	                                                       java.sql.Timestamp end) throws Exception {
	    String sql = """
	        SELECT reservation_time, expiry_time, num_of_customers
	        FROM reservation
	        WHERE status IN ('CONFIRMED','PENDING','ARRIVED')
	          AND reservation_time < ?
	          AND expiry_time > ?
	    """;

	    List<long[]> out = new ArrayList<>();
	    try (PreparedStatement ps = conn.prepareStatement(sql)) {
	        ps.setTimestamp(1, end);
	        ps.setTimestamp(2, start);
	        try (ResultSet rs = ps.executeQuery()) {
	            while (rs.next()) {
	                int p = rs.getInt(3);
	                if (p > 0) out.add(new long[] { rs.getTimestamp(1).getTime(), rs.getTimestamp(2).getTime(), p });
	            }
	        }
	    }
	    return out;
	}

	/**
	 * POLICY 1 core:
	 * Can the new party be seated for its whole stay [start, end) together with the reservations
	 * that are in the restaurant AT THE SAME TIME? Each reservation needs one whole table,
	 * a table is never shared between reservations.
	 * <p>
	 * Occupancy only increases when a reservation starts, so it is enough to check the moment the
	 * new party arrives and every reservation start inside its stay. (The old version packed every
	 * reservation that overlapped the 2-hour window at once, so a 17:00 and a 19:00 booking both
	 * "blocked" an 18:00 request even though they are never in the restaurant together.)
	 * <p>
	 * This does NOT touch restaurant_table.status (planning only).
	 */
	public static boolean canPackReservationsAtTime(Connection conn,
	                                                java.sql.Timestamp start,
	                                                java.sql.Timestamp end,
	                                                int newParty) throws Exception {
	    if (newParty <= 0) return false;

	    List<TableCandidate> tables = getAllTablesForPlanning(conn);
	    if (tables.isEmpty()) return false;

	    List<long[]> overlapping = getOverlappingReservations(conn, start, end);

	    List<Long> checkpoints = new ArrayList<>();
	    checkpoints.add(start.getTime());
	    for (long[] r : overlapping) {
	        if (r[0] > start.getTime() && r[0] < end.getTime()) checkpoints.add(r[0]);
	    }

	    for (long t : checkpoints) {
	        List<Integer> parties = new ArrayList<>();
	        parties.add(newParty);
	        for (long[] r : overlapping) {
	            if (r[0] <= t && r[1] > t) parties.add((int) r[2]);
	        }
	        if (!packs(tables, parties)) return false;
	    }
	    return true;
	}

	/**
	 * Keeps only the slots ("HH:mm") of {@code date} at which a party of {@code people} can be seated
	 * for {@code diningMinutes}. Uses one connection for all slots.
	 *
	 * @return the slots that still have room (same order)
	 * @throws Exception on database error
	 */
	public static List<String> filterSlotsWithRoom(java.time.LocalDate date, List<String> slots,
	                                               int people, int diningMinutes) throws Exception {
	    List<String> out = new ArrayList<>();
	    if (slots == null || slots.isEmpty() || people <= 0) return out;

	    MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
	    PooledConnection pc = pool.getConnection();
	    Connection conn = pc.getConnection();
	    try {
	        for (String slot : slots) {
	            java.time.LocalDateTime start = java.time.LocalDateTime.of(date, java.time.LocalTime.parse(slot));
	            java.sql.Timestamp s = java.sql.Timestamp.valueOf(start);
	            java.sql.Timestamp e = java.sql.Timestamp.valueOf(start.plusMinutes(diningMinutes));
	            if (canPackReservationsAtTime(conn, s, e, people)) out.add(slot);
	        }
	    } finally {
	        pool.releaseConnection(pc);
	    }
	    return out;
	}

	/**
	 * Convenience overload that opens its own connection.
	 */
	public static boolean canPackReservationsAtTime(java.sql.Timestamp start,
	                                                java.sql.Timestamp end,
	                                                int newParty) throws Exception {
	    MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
	    PooledConnection pc = pool.getConnection();
	    Connection conn = pc.getConnection();
	    try {
	        return canPackReservationsAtTime(conn, start, end, newParty);
	    } finally {
	        pool.releaseConnection(pc);
	    }
	}

	/**
	 * Simulates whether the current inventory of FREE tables can accommodate upcoming reservations.
	 * <p>
	 * This method performs a "dry run" allocation planning logic:
	 * </p>
	 * <ol>
	 * <li>Snapshots all currently FREE tables.</li>
	 * <li>Fetches the party sizes of upcoming reservations (within the look-ahead window).</li>
	 * <li>Iteratively attempts to match each reservation to the best-fit subset of free tables.</li>
	 * </ol>
	 * If the simulation successfully finds a seat configuration for every upcoming reservation
	 * without running out of tables, it returns {@code true}.
	 * This is useful for "Safe Delete" checks or capacity planning warnings.
	 * 
	 *
	 * @param conn the active database connection
	 * @param lookAheadMinutes how many minutes into the future to scan for upcoming reservations
	 * @param limitReservations maximum number of upcoming reservations to consider (for performance)
	 * @return {@code true} if all upcoming reservations can be seated with the current free tables; {@code false} otherwise.
	 * @throws Exception if a database access error occurs
	 */
	public static boolean canSeatUpcomingReservationsFromFreeTables(
	        Connection conn,
	        int lookAheadMinutes,
	        int limitReservations
	) throws Exception {

	    // 1) upcoming reservations that still need tables (ordered by time)
	    List<Integer> needs = ReservationDAO.getUpcomingReservationSizes(conn, lookAheadMinutes, limitReservations);

	    // Nothing to protect -> it's always safe. (Before this fix the method returned false whenever
	    // no FREE table was left, so a waiting party could never get the LAST free table(s).)
	    if (needs.isEmpty()) return true;

	    // 2) snapshot FREE tables (table_id + num_of_seats)
	    List<TableCandidate> free = getFreeTablesForPlanning(conn);
	    if (free.isEmpty()) return false;

	    // 3) every upcoming party must still get its own free table
	    return packs(free, needs);
	}

	/**
	 * Retrieves a snapshot of all currently FREE tables for planning purposes.
	 * <p>
	 * Returns a list of {@link TableCandidate} objects containing the ID and seat count
	 * for every table with status 'FREE', sorted by seat count in ascending order.
	 * This list is typically used by simulation algorithms (like {@link #canSeatUpcomingReservationsFromFreeTables})
	 * to test allocation strategies without modifying the actual database.
	 * </p>
	 *
	 * @param conn the active database connection
	 * @return a list of table candidates representing the current free inventory
	 * @throws Exception if a database access error occurs
	 */
	private static List<TableCandidate> getFreeTablesForPlanning(Connection conn) throws Exception {
	    List<TableCandidate> list = new ArrayList<>();
	    try (PreparedStatement ps = conn.prepareStatement("""
	        SELECT table_id, num_of_seats
	        FROM restaurant_table
	        WHERE status = 'FREE'
	        ORDER BY num_of_seats ASC
	    """);
	         ResultSet rs = ps.executeQuery()) {
	        while (rs.next()) {
	            list.add(new TableCandidate(rs.getString("table_id"), rs.getInt("num_of_seats")));
	        }
	    }
	    return list;
	}
	
	/**
	 * Checks if a table is safe to be deleted from the system.
	 * <p>
	 * Performs three distinct checks:
	 * 1. <b>Status Check:</b> Ensures the table is currently "FREE" (not OCCUPIED or RESERVED).
	 * 2. <b>Active Visit Check:</b> Ensures no customers are currently seated (linked via the visit table).
	 * 3. <b>Future Capacity Check:</b> Ensures that removing this table will not cause overbooking
	 * for any future reservation slot where the total booked guests would exceed the new total capacity.
	 * </p>
	 *
	 * @param tableId the ID of the table to check
	 * @return {@code null} if safe to delete; otherwise, a {@code String} containing the error message explaining the conflict.
	 * @throws Exception if a database access error occurs
	 */
    public static String isSafeToDelete(String tableId) throws Exception {
        return checkTableChange(tableId, 0);
    }

    /**
     * Checks if the seat count of a table can be changed without breaking existing reservations.
     *
     * @param tableId  table to change
     * @param newSeats new seat count (must be positive)
     * @return {@code null} if safe; otherwise an error message explaining the conflict
     * @throws Exception if a database access error occurs
     */
    public static String isSafeToChangeSeats(String tableId, int newSeats) throws Exception {
        if (newSeats <= 0) return "Seats must be positive.";
        return checkTableChange(tableId, newSeats);
    }

    /**
     * Shared safety check for deleting a table ({@code newSeats == 0}) or changing its seats.
     * <ol>
     *   <li>The table must exist and be FREE (not occupied / held for a customer).</li>
     *   <li>No active visit may use it.</li>
     *   <li>Every current/future reservation must still be seatable with the changed inventory.
     *       This uses the same one-table-per-party logic as booking, at every moment where the set
     *       of overlapping reservations changes (not just a seat-count sum).</li>
     * </ol>
     */
    private static String checkTableChange(String tableId, int newSeats) throws Exception {
        if (tableId == null || tableId.isBlank()) return "Missing table id.";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            // 1. Current status
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT status FROM restaurant_table WHERE table_id = ?")) {
                ps.setString(1, tableId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return "Table not found.";
                    String status = rs.getString("status");
                    if (!"FREE".equalsIgnoreCase(status)) {
                        return "Table " + tableId + " is currently " + status + ". Try again when it is FREE.";
                    }
                }
            }

            // 2. Active visits (double safety)
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM visit WHERE table_id = ? AND actual_end_time IS NULL")) {
                ps.setString(1, tableId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        return "Table is currently occupied by active diners.";
                    }
                }
            }

            // 3. Future reservations must still fit
            List<TableCandidate> inventory = getAllTablesForPlanning(conn);
            inventory.removeIf(t -> t.tableId.equals(tableId));
            if (newSeats > 0) inventory.add(new TableCandidate(tableId, newSeats));

            List<long[]> reservations = new ArrayList<>(); // {startMs, endMs, party}
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT reservation_time, expiry_time, num_of_customers
                    FROM reservation
                    WHERE status IN ('CONFIRMED','PENDING','ARRIVED')
                      AND expiry_time > NOW()
                    ORDER BY reservation_time
                """);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    reservations.add(new long[] {
                            rs.getTimestamp(1).getTime(), rs.getTimestamp(2).getTime(), rs.getInt(3) });
                }
            }

            for (long[] r : reservations) {
                long t = r[0];
                List<Integer> parties = new ArrayList<>();
                for (long[] other : reservations) {
                    if (other[0] <= t && other[1] > t) parties.add((int) other[2]);
                }
                if (!packs(inventory, parties)) {
                    return "Cannot " + (newSeats == 0 ? "delete" : "change") + " table " + tableId
                            + ": reservations at " + new java.sql.Timestamp(t).toLocalDateTime().toString().replace('T', ' ')
                            + " could no longer be seated.";
                }
            }

            return null; // safe
        } finally {
            pool.releaseConnection(pc);
        }
    }

    /**
     * True if all parties can be seated at the same time in the given tables
     * Each party needs ONE whole table with enough seats, and a table is never shared (spec).
     * <p>
     * Biggest parties first, each taking the smallest table that still fits: for this
     * "one party - one table with seats >= party" problem the greedy order is optimal.
     */
    private static boolean packs(List<TableCandidate> tables, List<Integer> parties) {
        List<TableCandidate> remaining = new ArrayList<>(tables);
        List<Integer> sorted = new ArrayList<>(parties);
        sorted.sort(Collections.reverseOrder()); // biggest parties first
        for (int party : sorted) {
            if (party <= 0) continue;
            TableCandidate chosen = chooseTable(remaining, party);
            if (chosen == null) return false;
            remaining.remove(chosen);
        }
        return true;
    }
    
    /**
     * Inserts a new table into the database with an automatically generated ID.
     * <p>
     * This method calculates the next available ID in the sequence (e.g., T10 -> T11)
     * by finding the current maximum numeric suffix in the 'restaurant_table' table.
     * It is thread-safe regarding the connection pool but does not lock the table for strict serialization.
     * </p>
     *
     * @param seats the number of seats for the new table
     * @return the generated table ID (e.g., "T11")
     * @throws Exception if a database access error occurs or generation fails
     */
    public static synchronized String insertTableAutoId(int seats) throws Exception {
        // synchronized: "find max id" + "insert" must not interleave between two staff clients
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            // 1. Find the highest existing ID (Assuming format 'T' + number, e.g., T1, T2, T10)
            // We cast the substring to UNSIGNED to ensure T10 comes after T2
            String sqlMax = """
                SELECT table_id 
                FROM restaurant_table 
                WHERE table_id LIKE 'T%' 
                ORDER BY CAST(SUBSTRING(table_id, 2) AS UNSIGNED) DESC 
                LIMIT 1
            """;
            
            String nextId = "T1"; // Default if empty

            try (PreparedStatement ps = conn.prepareStatement(sqlMax);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String currentMax = rs.getString(1);
                    try {
                        // Extract number part (e.g. "10" from "T10")
                        int num = Integer.parseInt(currentMax.substring(1));
                        nextId = "T" + (num + 1);
                    } catch (NumberFormatException e) {
                        // Fallback if ID format is weird, just append timestamp or random
                        nextId = "T" + (System.currentTimeMillis() % 10000);
                    }
                }
            }

            // 2. Insert the new table
            String sqlInsert = "INSERT INTO restaurant_table (table_id, num_of_seats, status) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
                ps.setString(1, nextId);
                ps.setInt(2, seats);
                ps.setString(3, "FREE");
                ps.executeUpdate();
            }
            
            return nextId;

        } finally {
            pool.releaseConnection(pc);
        }
    }


}