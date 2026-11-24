package DataBase.dao;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
import common.dto.BillDTO;


import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * Data Access Object (DAO) for handling billing operations.
 *
 * Responsibilities:
 * <ul>
 *   <li>Locate bills by reservation or waiting-list confirmation code</li>
 *   <li>Create bills on demand for active visits</li>
 *   <li>Process bill payments using database transactions</li>
 *   <li>Apply subscriber discounts when applicable</li>
 *   <li>Close visits and release occupied tables after payment</li>
 *   <li>Support reminder queries for unpaid bills</li>
 * </ul>
 *
 * All database access is performed using {@link MySQLConnectionPool}.
 * Transaction safety (commit / rollback) is enforced for payment operations.
 */
public class BillDAO {

    // =========================
    // Existing insert (kept)
    // =========================
    /**
     * Inserts a new bill record for a given visit.
     *
     * @param visitId  the visit identifier
     * @param amount   total bill amount
     * @param discount subscriber discount flag ("YES"/"NO")
     * @param paid     payment status ("YES"/"NO")
     * @throws Exception if database operation fails
     */
    public static void insertBill(int visitId, double amount, String discount, String paid) throws Exception {
        String sql = """
            INSERT INTO bill (visit_id, total_amount, is_subscriber_discount, is_paid)
            VALUES (?, ?, ?, ?)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, visitId);
            ps.setDouble(2, amount);
            ps.setString(3, discount);
            ps.setString(4, paid);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }

    // =========================================================
    // 1) Lookup bill by confirmation code (reservation OR waiting)
    // =========================================================
    /**
     * Retrieves a bill by confirmation code (reservation or waiting list).
     * <p>
     * If no bill exists for the active visit, a new bill is created on demand.
     * Handles cases where the bill is already paid.
     *
     * @param code confirmation code
     * @return lookup result containing bill data or error status
     * @throws Exception if database access fails
     */
    public static BillLookupResult getBillByConfirmationCode(String code) throws Exception {
        if (code == null) code = "";
        code = code.trim();
        if (code.isBlank()) return BillLookupResult.notFound("Empty code.");

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            // 1) Resolve code -> reservation_id or waiting_id
            Integer reservationId = findReservationIdByCode(conn, code);
            Integer waitingId = (reservationId == null) ? findWaitingIdByCode(conn, code) : null;

            if (reservationId == null && waitingId == null) {
                return BillLookupResult.notFound("Code not found.");
            }

            // 2) Resolve -> user_activity row
            UserActivityRow ua = findUserActivity(conn, reservationId, waitingId);
            if (ua == null) {
                return BillLookupResult.notFound("No activity found for this code.");
            }

            // 3) Find ACTIVE visit (actual_end_time IS NULL)
            VisitRow v = findActiveVisit(conn, ua.activityId);
            if (v == null) {
                // maybe it was already paid / ended; show "already paid" if so
                VisitRow last = findLastVisit(conn, ua.activityId);
                if (last == null) return BillLookupResult.notFound("No visit found for this code.");

                BillRow b2 = findLatestBillByActivity(conn, ua.activityId);
                if (b2 != null && "YES".equalsIgnoreCase(b2.isPaid)) {
                    BillDTO dtoPaid = buildBillDTO(ua, last, b2);
                    return BillLookupResult.alreadyPaid(dtoPaid, "Bill already paid.");
                }

                return BillLookupResult.notFound("No active visit for this code (not checked-in or already ended).");
            }

            // 4) One bill per party: look it up by ACTIVITY (a multi-table party has several visits,
            //    looking up by one visit could create a second bill for the same party).
            BillRow b = getOrCreateBillForActivity(conn, ua.activityId, reservationId, waitingId);
            if (b == null) {
                return BillLookupResult.notFound("Failed to create bill for this visit.");
            }


            BillDTO dto = buildBillDTO(ua, v, b);

            boolean paid = "YES".equalsIgnoreCase(b.isPaid);
            if (paid) return BillLookupResult.alreadyPaid(dto, "Bill already paid.");

            return BillLookupResult.ok(dto, "Bill found ✅");

        } finally {
            pool.releaseConnection(pc);
        }
    }

    // =========================================================
    // 2) Pay bill by confirmation code (transaction)
    //    - mark bill paid
    //    - end visit (actual_end_time = NOW())
    //    - free table (status = FREE)
    // =========================================================

    /**
     * Processes payment for a bill identified by confirmation code.
     * <p>
     * This method performs a transactional operation:
     * <ul>
     *   <li>Marks the bill as paid</li>
     *   <li>Ends all active visits for the activity</li>
     *   <li>Releases occupied tables</li>
     *   <li>Updates reservation or waiting-list status</li>
     * </ul>
     *
     * @param code confirmation code
     * @return payment result
     * @throws Exception if payment fails or database error occurs
     */
    public static PayBillResult payBillByConfirmationCode(String code) throws Exception {
        if (code == null) code = "";
        code = code.trim();
        if (code.isBlank()) return PayBillResult.fail("Empty code.");

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            conn.setAutoCommit(false);

            Integer reservationId = findReservationIdByCode(conn, code);
            Integer waitingId = (reservationId == null) ? findWaitingIdByCode(conn, code) : null;

            if (reservationId == null && waitingId == null) {
                conn.rollback();
                return PayBillResult.fail("Code not found.");
            }

            // ✅ Enforce correct "can pay" status before doing anything
            if (reservationId != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT status FROM reservation WHERE reservation_id = ? LIMIT 1
                """)) {
                    ps.setInt(1, reservationId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            return PayBillResult.fail("Reservation not found.");
                        }
                        String st = rs.getString(1);
                        if (!"ARRIVED".equalsIgnoreCase(st)) {
                            conn.rollback();
                            return PayBillResult.fail(
                                    "Pay allowed only after check-in (reservation must be ARRIVED). Current: " + st
                            );
                        }
                    }
                }
            }

            if (waitingId != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT status FROM waiting_list WHERE waiting_id = ? LIMIT 1
                """)) {
                    ps.setInt(1, waitingId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            return PayBillResult.fail("Waiting list not found.");
                        }
                        String st = rs.getString(1);
                        if (!"ARRIVED".equalsIgnoreCase(st)) {
                            conn.rollback();
                            return PayBillResult.fail(
                                    "Pay allowed only after check-in (waiting list must be ARRIVED). Current: " + st
                            );
                        }
                    }
                }
            }

            UserActivityRow ua = findUserActivity(conn, reservationId, waitingId);
            if (ua == null) {
                conn.rollback();
                return PayBillResult.fail("No activity found for this code.");
            }

            // find any active visit (there may be MANY in Option B)
            VisitRow v = findActiveVisit(conn, ua.activityId);
            if (v == null) {
                conn.rollback();
                return PayBillResult.fail("No active visit found for this code.");
            }

            // Bill is found by ACTIVITY (one bill per party). Create it if nobody opened it yet.
            BillRow b = getOrCreateBillForActivity(conn, ua.activityId, reservationId, waitingId);
            if (b == null) {
                conn.rollback();
                return PayBillResult.fail("Could not create the bill for this visit.");
            }

            if ("YES".equalsIgnoreCase(b.isPaid)) {
                conn.rollback();
                return PayBillResult.fail("Bill already paid.");
            }

            // compute final amount (10% for subscriber), rounded to agorot
            boolean subscriber = (ua.subscriberUsername != null && !ua.subscriberUsername.isBlank());
            double[] amounts = computeAmounts(b.totalAmount, subscriber);
            double total = amounts[2];

            // 1) Update bill
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE bill
                SET is_paid = 'YES',
                    is_subscriber_discount = ?,
                    total_amount = ?
                WHERE bill_id = ? AND is_paid = 'NO'
            """)) {
                ps.setString(1, subscriber ? "YES" : "NO");
                ps.setDouble(2, total);
                ps.setInt(3, b.billId);
                int updated = ps.executeUpdate();
                if (updated != 1) {
                    conn.rollback();
                    return PayBillResult.fail("Pay failed (bill state changed).");
                }
            }

            // =========================================================
            // ✅ OPTION B FIX #1: End ALL active visits of this activity
            // =========================================================
            int endedVisits;
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE visit
                SET actual_end_time = NOW()
                WHERE activity_id = ?
                  AND actual_end_time IS NULL
            """)) {
                ps.setInt(1, ua.activityId);
                endedVisits = ps.executeUpdate();
                if (endedVisits <= 0) {
                    conn.rollback();
                    return PayBillResult.fail("Pay failed (no active visits to end).");
                }
            }

            // =========================================================
            // ✅ OPTION B FIX #2: Free ALL tables used by the reservation OR waiting list
            // =========================================================
            String releasedTablesInfo;

            if (reservationId != null) {
                int freed = RestaurantTableDAO.freeOccupiedTablesForReservation(conn, reservationId);
                if (freed <= 0) {
                    conn.rollback();
                    return PayBillResult.fail("Pay failed (no occupied tables found for reservation).");
                }
                releasedTablesInfo = "Released " + freed + " table(s)";
            } else {
                // ✅ Waiting list Option B: free ALL occupied tables for waitingId
                int freed = RestaurantTableDAO.freeOccupiedTablesForWaiting(conn, waitingId);
                if (freed <= 0) {
                    conn.rollback();
                    return PayBillResult.fail("Pay failed (no occupied tables found for waiting list).");
                }
                releasedTablesInfo = "Released " + freed + " table(s)";
            }

            // 4) Finish reservation after payment: ARRIVED -> EXPIRED
            if (reservationId != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE reservation
                    SET status = 'EXPIRED'
                    WHERE reservation_id = ?
                      AND status = 'ARRIVED'
                """)) {
                    ps.setInt(1, reservationId);
                    int updated = ps.executeUpdate();
                    if (updated != 1) {
                        conn.rollback();
                        return PayBillResult.fail("Pay failed (reservation not ARRIVED / already finished).");
                    }
                }
            }

            // 5) Finish waiting list after payment: ARRIVED -> EXPIRED
            if (waitingId != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE waiting_list
                    SET status = 'EXPIRED'
                    WHERE waiting_id = ?
                      AND status = 'ARRIVED'
                """)) {
                    ps.setInt(1, waitingId);
                    int updated = ps.executeUpdate();
                    if (updated != 1) {
                        conn.rollback();
                        return PayBillResult.fail("Pay failed (waiting list not ARRIVED / already finished).");
                    }
                }
            }

            conn.commit();

            // Keep compatibility with UI (tableId still returned, though multi-table exists)
            // For multi-table, we still return the main table from the active visit.
            return PayBillResult.ok("Payment successful ✅ — " + releasedTablesInfo, v.tableId);

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignored) {}
            return PayBillResult.fail("Server error: " + e.getMessage());
        } finally {
            try { conn.setAutoCommit(true); } catch (Exception ignored) {}
            pool.releaseConnection(pc);
        }
    }

    // =========================
    // Helpers (private)
    // =========================
    
    private static BillRow findLatestBillByActivity(Connection conn, int activityId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT b.bill_id, b.visit_id, b.total_amount, b.is_subscriber_discount, b.is_paid
            FROM bill b
            JOIN visit v ON v.visit_id = b.visit_id
            WHERE v.activity_id = ?
            ORDER BY b.bill_id DESC
            LIMIT 1
        """)) {
            ps.setInt(1, activityId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new BillRow(
                        rs.getInt("bill_id"),
                        rs.getInt("visit_id"),
                        rs.getDouble("total_amount"),
                        rs.getString("is_subscriber_discount"),
                        rs.getString("is_paid")
                );
            }
        }
    }


    /** Guards "find bill / create bill" so two lookups at once don't create two bills for one party. */
    private static final Object BILL_LOCK = new Object();

    /**
     * Returns the party's bill, creating it (on its active visit) if it does not exist yet.
     *
     * @return the bill, or null if the party has no active visit
     */
    private static BillRow getOrCreateBillForActivity(Connection conn, int activityId,
                                                      Integer reservationId, Integer waitingId) throws Exception {
        synchronized (BILL_LOCK) {
            BillRow b = findLatestBillByActivity(conn, activityId);
            if (b != null) return b;

            VisitRow v = findActiveVisit(conn, activityId);
            if (v == null) return null;

            double subtotal = computeSubtotalForCode(conn, reservationId, waitingId);
            try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO bill (visit_id, total_amount, is_subscriber_discount, is_paid)
                VALUES (?, ?, 'NO', 'NO')
            """)) {
                ps.setInt(1, v.visitId);
                ps.setDouble(2, subtotal);
                ps.executeUpdate();
            }
            return findLatestBillByActivity(conn, activityId);
        }
    }

    /**
     * Creates bills for parties that have been seated for 2+ hours but whose bill was never opened.
     * <p>
     * Bills used to be created only when someone clicked "Find Bill", so the 2-hour payment reminder
     * (which looks for unpaid bills) never fired for those parties.
     *
     * @return number of bills created
     * @throws Exception on database error
     */
    public static int createMissingBillsForOverdueVisits() throws Exception {
        String sql = """
            SELECT ua.activity_id, ua.reservation_id, ua.waiting_id
            FROM user_activity ua
            WHERE EXISTS (
                    SELECT 1 FROM visit v
                    WHERE v.activity_id = ua.activity_id
                      AND v.actual_end_time IS NULL
                      AND v.actual_start_time <= (NOW() - INTERVAL 2 HOUR))
              AND NOT EXISTS (
                    SELECT 1 FROM bill b
                    JOIN visit v2 ON v2.visit_id = b.visit_id
                    WHERE v2.activity_id = ua.activity_id)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            List<int[]> rows = new ArrayList<>(); // {activityId, reservationId or -1, waitingId or -1}
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int activityId = rs.getInt(1);
                    int resId = rs.getInt(2);
                    if (rs.wasNull()) resId = -1;
                    int waitId = rs.getInt(3);
                    if (rs.wasNull()) waitId = -1;
                    rows.add(new int[] { activityId, resId, waitId });
                }
            }

            int created = 0;
            for (int[] r : rows) {
                BillRow b = getOrCreateBillForActivity(conn, r[0],
                        r[1] > 0 ? r[1] : null, r[2] > 0 ? r[2] : null);
                if (b != null) created++;
            }
            return created;
        } finally {
            pool.releaseConnection(pc);
        }
    }

    private static Integer findReservationIdByCode(Connection conn, String code) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT reservation_id FROM reservation WHERE confirmation_code = ?
        """)) {
            ps.setString(1, code);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        }
    }

    private static Integer findWaitingIdByCode(Connection conn, String code) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT waiting_id FROM waiting_list WHERE confirmation_code = ?
        """)) {
            ps.setString(1, code);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        }
    }

    private static UserActivityRow findUserActivity(Connection conn, Integer reservationId, Integer waitingId) throws Exception {
        if (reservationId != null) {
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT activity_id, subscriber_username, guest_email, guest_phone
                FROM user_activity
                WHERE reservation_id = ?
                ORDER BY activity_date DESC
                LIMIT 1
            """)) {
                ps.setInt(1, reservationId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    return new UserActivityRow(
                            rs.getInt("activity_id"),
                            rs.getString("subscriber_username"),
                            rs.getString("guest_email"),
                            rs.getString("guest_phone")
                    );
                }
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT activity_id, subscriber_username, guest_email, guest_phone
                FROM user_activity
                WHERE waiting_id = ?
                ORDER BY activity_date DESC
                LIMIT 1
            """)) {
                ps.setInt(1, waitingId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    return new UserActivityRow(
                            rs.getInt("activity_id"),
                            rs.getString("subscriber_username"),
                            rs.getString("guest_email"),
                            rs.getString("guest_phone")
                    );
                }
            }
        }
    }

    private static VisitRow findActiveVisit(Connection conn, int activityId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT visit_id, table_id, actual_start_time, actual_end_time
            FROM visit
            WHERE activity_id = ?
              AND actual_end_time IS NULL
            ORDER BY actual_start_time DESC
            LIMIT 1
        """)) {
            ps.setInt(1, activityId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new VisitRow(
                        rs.getInt("visit_id"),
                        rs.getString("table_id"),
                        rs.getTimestamp("actual_start_time"),
                        rs.getTimestamp("actual_end_time")
                );
            }
        }
    }

    private static VisitRow findLastVisit(Connection conn, int activityId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT visit_id, table_id, actual_start_time, actual_end_time
            FROM visit
            WHERE activity_id = ?
            ORDER BY actual_start_time DESC
            LIMIT 1
        """)) {
            ps.setInt(1, activityId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new VisitRow(
                        rs.getInt("visit_id"),
                        rs.getString("table_id"),
                        rs.getTimestamp("actual_start_time"),
                        rs.getTimestamp("actual_end_time")
                );
            }
        }
    }

    /** Subscriber discount rate (10%). */
    private static final java.math.BigDecimal DISCOUNT_RATE = new java.math.BigDecimal("0.10");

    /**
     * Computes {subtotal, discount, total} from a subtotal, with money rounding (2 decimals, HALF_UP).
     */
    private static double[] computeAmounts(double subtotalValue, boolean subscriber) {
        java.math.BigDecimal subtotal = java.math.BigDecimal.valueOf(subtotalValue)
                .setScale(2, java.math.RoundingMode.HALF_UP);
        java.math.BigDecimal discount = subscriber
                ? subtotal.multiply(DISCOUNT_RATE).setScale(2, java.math.RoundingMode.HALF_UP)
                : java.math.BigDecimal.ZERO.setScale(2);
        java.math.BigDecimal total = subtotal.subtract(discount);
        return new double[] { subtotal.doubleValue(), discount.doubleValue(), total.doubleValue() };
    }

    private static BillDTO buildBillDTO(UserActivityRow ua, VisitRow v, BillRow b) {
        boolean subscriber = (ua.subscriberUsername != null && !ua.subscriberUsername.isBlank());

        double subtotal;
        double discount;
        double total;
        if ("YES".equalsIgnoreCase(b.isPaid) && "YES".equalsIgnoreCase(b.isSubscriberDiscount)) {
            // A paid subscriber bill stores the FINAL (already discounted) amount.
            // Rebuild the subtotal instead of discounting a second time.
            java.math.BigDecimal paid = java.math.BigDecimal.valueOf(b.totalAmount).setScale(2, java.math.RoundingMode.HALF_UP);
            java.math.BigDecimal sub = paid.divide(java.math.BigDecimal.ONE.subtract(DISCOUNT_RATE), 2, java.math.RoundingMode.HALF_UP);
            subtotal = sub.doubleValue();
            discount = sub.subtract(paid).doubleValue();
            total = paid.doubleValue();
        } else if ("YES".equalsIgnoreCase(b.isPaid)) {
            subtotal = b.totalAmount;
            discount = 0.0;
            total = b.totalAmount;
        } else {
            double[] a = computeAmounts(b.totalAmount, subscriber);
            subtotal = a[0];
            discount = a[1];
            total = a[2];
        }

        String customerName = subscriber ? ua.subscriberUsername : (ua.guestEmail != null && !ua.guestEmail.isBlank() ? ua.guestEmail : "Guest");

        // due date = start + 2 hours (computed)
        String due = "-";
        if (v.start != null) {
            LocalDateTime dueLdt = v.start.toLocalDateTime().plusHours(2);
            due = dueLdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        }

        BillDTO dto = new BillDTO();
        dto.setCustomerName(customerName);
        dto.setItemsCount(1);
        dto.setSubtotal(subtotal);
        dto.setDiscount(discount);
        dto.setTotal(total);
        dto.setDueDate(due);
        dto.setSubscriberDiscountApplied(subscriber);

        return dto;
    }
    
    private static double computeSubtotalForCode(Connection conn, Integer reservationId, Integer waitingId) throws Exception {
        final double PRICE_PER_PERSON = 100.0; // <-- change if you want

        if (reservationId != null) {
            try (PreparedStatement ps = conn.prepareStatement("""
                SELECT num_of_customers FROM reservation WHERE reservation_id = ?
            """)) {
                ps.setInt(1, reservationId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int ppl = rs.getInt(1);
                        return Math.max(0, ppl) * PRICE_PER_PERSON;
                    }
                }
            }
            return 0.0;
        }

        // waiting list
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT num_of_customers FROM waiting_list WHERE waiting_id = ?
        """)) {
            ps.setInt(1, waitingId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int ppl = rs.getInt(1);
                    return Math.max(0, ppl) * PRICE_PER_PERSON;
                }
            }
        }
        return 0.0;
    }


    // =========================
    // Small structs
    // =========================
    private record UserActivityRow(int activityId, String subscriberUsername, String guestEmail, String guestPhone) {}
    private record VisitRow(int visitId, String tableId, Timestamp start, Timestamp end) {}
    private record BillRow(int billId, int visitId, double totalAmount, String isSubscriberDiscount, String isPaid) {}

    // =========================
    // Results (simple)
    // =========================

    /**
     * Result wrapper for bill lookup operations.
     */
    public static class BillLookupResult {
        public final boolean ok;
        public final boolean alreadyPaid;
        public final String message;
        public final BillDTO bill;

        private BillLookupResult(boolean ok, boolean alreadyPaid, String message, BillDTO bill) {
            this.ok = ok;
            this.alreadyPaid = alreadyPaid;
            this.message = message;
            this.bill = bill;
        }

        public static BillLookupResult ok(BillDTO bill, String msg) { return new BillLookupResult(true, false, msg, bill); }
        public static BillLookupResult alreadyPaid(BillDTO bill, String msg) { return new BillLookupResult(true, true, msg, bill); }
        public static BillLookupResult notFound(String msg) { return new BillLookupResult(false, false, msg, null); }
    }

    /**
     * Result wrapper for payment operations.
     */
    public static class PayBillResult {
        public final boolean ok;
        public final String message;
        public final String tableId;

        private PayBillResult(boolean ok, String message, String tableId) {
            this.ok = ok;
            this.message = message;
            this.tableId = tableId;
        }

        public static PayBillResult ok(String msg, String tableId) { return new PayBillResult(true, msg, tableId); }
        public static PayBillResult fail(String msg) { return new PayBillResult(false, msg, null); }
    }

    /**
     * Lightweight record used for reminder processing.
     */
    public static class BillReminderRow {
        public final int billId;
        public final String email;
        public final String phone;
        public final String confirmationCode;
        /** Bill details for the 2-hour message (spec: "a message with the bill details"). */
        public final int people;
        public final double subtotal;
        public final double discount;
        public final double total;

        public BillReminderRow(int billId, String email, String phone, String confirmationCode,
                               int people, double subtotal, double discount, double total) {
            this.billId = billId;
            this.email = email;
            this.phone = phone;
            this.confirmationCode = confirmationCode;
            this.people = people;
            this.subtotal = subtotal;
            this.discount = discount;
            this.total = total;
        }
    }

    public static List<BillReminderRow> findBillsNeedingReminder(int limit) throws Exception {
    	String sql = """
    		    SELECT
    		        b.bill_id,
    		        b.total_amount,
    		        (ua.subscriber_username IS NOT NULL) AS is_subscriber,
    		        COALESCE(r.num_of_customers, wl.num_of_customers, 0) AS people,
    		        COALESCE(s.email, ua.guest_email) AS email,
    		        COALESCE(s.phone, ua.guest_phone) AS phone,
    		        COALESCE(r.confirmation_code, wl.confirmation_code) AS code
    		    FROM bill b
    		    JOIN visit v ON v.visit_id = b.visit_id
    		    JOIN user_activity ua ON ua.activity_id = v.activity_id
    		    LEFT JOIN subscribers s ON s.username = ua.subscriber_username
    		    LEFT JOIN reservation r ON r.reservation_id = ua.reservation_id
    		    LEFT JOIN waiting_list wl ON wl.waiting_id = ua.waiting_id
    		    WHERE b.is_paid = 'NO'
    		      AND b.reminder_sent = 'NO'
    		      AND v.actual_end_time IS NULL
    		      AND v.actual_start_time <= (NOW() - INTERVAL 2 HOUR)
    		      -- at least one way to reach the customer (email, or phone for the SMS)
    		      AND (COALESCE(s.email, ua.guest_email, '') <> '' OR COALESCE(s.phone, ua.guest_phone, '') <> '')
    		      AND COALESCE(r.confirmation_code, wl.confirmation_code) IS NOT NULL
    		      AND COALESCE(r.confirmation_code, wl.confirmation_code) <> ''
    		    ORDER BY v.actual_start_time ASC
    		    LIMIT ?
    		""";


        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);

            List<BillReminderRow> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    double[] a = computeAmounts(rs.getDouble("total_amount"), rs.getInt("is_subscriber") == 1);
                    out.add(new BillReminderRow(
                            rs.getInt("bill_id"),
                            rs.getString("email"),
                            rs.getString("phone"),
                            rs.getString("code"),
                            rs.getInt("people"),
                            a[0], a[1], a[2]
                    ));
                }
            }
            return out;
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Marks a bill reminder as sent.
     * <p>
     * Updates the bill record by setting the reminder flag and
     * storing the timestamp of when the reminder was sent.
     * This method is typically used after sending an email or SMS reminder.
     *
     * @param billId the identifier of the bill to update
     * @throws Exception if a database error occurs
     */
    public static void markReminderSent(int billId) throws Exception {
        String sql = """
            UPDATE bill
            SET reminder_sent = 'YES',
                reminder_sent_at = NOW()
            WHERE bill_id = ?
              AND reminder_sent = 'NO'
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, billId);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }

}

