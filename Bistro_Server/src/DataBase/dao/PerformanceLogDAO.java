package DataBase.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.sql.Date;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
/**
 * Data Access Object for performance and behavior logging.
 * <p>
 * Stores visit-related metrics such as late arrival,
 * overstay duration, and monthly performance data
 * used for management reports.
 */
public class PerformanceLogDAO {
	/**
	 * Inserts a performance log entry for a completed visit.
	 * <p>
	 * Used when visit timing data (late / overstay) is known explicitly.
	 *
	 * @param visitId         visit identifier
	 * @param lateMinutes     minutes late (0 if on time)
	 * @param overstayMinutes minutes beyond allowed duration
	 * @param reportDate      date associated with the report
	 * @throws Exception if a database error occurs
	 */
    public static void insertLog(
            int visitId,
            int lateMinutes,
            int overstayMinutes,
            Date reportDate) throws Exception {

        String sql = """
            INSERT INTO performance_log
            (visit_id, late_minutes, overstay_minutes, report_date)
            VALUES (?, ?, ?, ?)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, visitId);
            ps.setInt(2, lateMinutes);
            ps.setInt(3, overstayMinutes);
            ps.setDate(4, reportDate);
            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Generates performance log entries for all completed visits of past months.
     * <p>
     * Calculates late arrivals and overstays for completed visits and avoids duplicates.
     * <ul>
     *   <li>{@code report_date} is the DAY OF THE VISIT (it used to be the day the job ran, which put
     *       last month's data under the current month in the manager report).</li>
     *   <li>Covers every completed month that is not logged yet, so a month is not lost if the
     *       server was down when the month changed.</li>
     *   <li>A party seated at several tables has several visit rows; only one row per party
     *       (the first visit of the activity) is logged, so minutes are not counted twice.</li>
     * </ul>
     *
     * @return number of log rows inserted
     */
    public static int generateMonthlyReport() {
        int rowsAffected = 0;

        String sql = """
            INSERT INTO performance_log (visit_id, late_minutes, overstay_minutes, report_date)
            SELECT
                v.visit_id,

                -- Late minutes (only for reservations)
                CASE
                    WHEN r.reservation_time IS NOT NULL AND v.actual_start_time > r.reservation_time
                    THEN TIMESTAMPDIFF(MINUTE, r.reservation_time, v.actual_start_time)
                    ELSE 0
                END AS late_calc,

                -- Overstay minutes (stay longer than 120 minutes)
                CASE
                    WHEN TIMESTAMPDIFF(MINUTE, v.actual_start_time, v.actual_end_time) > 120
                    THEN TIMESTAMPDIFF(MINUTE, v.actual_start_time, v.actual_end_time) - 120
                    ELSE 0
                END AS overstay_calc,

                DATE(v.actual_start_time) AS visit_day

            FROM visit v
            JOIN user_activity ua ON v.activity_id = ua.activity_id
            LEFT JOIN reservation r ON ua.reservation_id = r.reservation_id
            WHERE
                -- 1. Visit is completed, and finished before the current month started
                v.actual_end_time IS NOT NULL
                AND v.actual_end_time < DATE_SUB(CURRENT_DATE, INTERVAL DAYOFMONTH(CURRENT_DATE) - 1 DAY)

                -- 2. One row per party (multi-table parties have several visit rows)
                AND v.visit_id = (SELECT MIN(v2.visit_id) FROM visit v2 WHERE v2.activity_id = v.activity_id)

                -- 3. Avoid duplicates: party not logged yet
                AND NOT EXISTS (
                    SELECT 1 FROM performance_log pl
                    JOIN visit v3 ON v3.visit_id = pl.visit_id
                    WHERE v3.activity_id = v.activity_id
                )
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = null;

        try {
            pc = pool.getConnection();
            Connection conn = pc.getConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                rowsAffected = ps.executeUpdate();
            }
        } catch (Exception e) {
            System.out.println("[Report] Error generating monthly report: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (pc != null) pool.releaseConnection(pc);
        }
        
        return rowsAffected;
    }
}

   

