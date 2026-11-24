package DataBase.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import DataBase.MySQLConnectionPool;
import DataBase.PooledConnection;
import common.dto.OpeningHoursDTO;
/**
 * Data Access Object for managing restaurant opening hours.
 * <p>
 * Handles weekly opening hours, special dates, and provides
 * logic utilities for reservation availability and time slots.
 */
public class OpeningHoursDAO {

    // =============================================================
    // 1. INSERT RAW (Used by Importer)
    // =============================================================
	/**
	 * Inserts a raw opening-hours record into the database.
	 * <p>
	 * Used mainly by data importers or initial system setup.
	 *
	 * @param dayOfWeek   day name (e.g. "Sunday")
	 * @param openTime    opening time
	 * @param closeTime   closing time
	 * @param isSpecial   "YES" if special date, otherwise "NO"
	 * @param specialDate specific date for special hours, or null
	 * @throws Exception if a database error occurs
	 */
    public static void insertOpeningHours(
            String dayOfWeek,
            Time openTime,
            Time closeTime,
            String isSpecial,
            Date specialDate) throws Exception {

        String sql = """
            INSERT INTO opening_hours
            (day_of_week, open_time, close_time, is_special, special_date)
            VALUES (?, ?, ?, ?, ?)
        """;

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dayOfWeek);
            ps.setTime(2, openTime);
            ps.setTime(3, closeTime);
            ps.setString(4, isSpecial);

            if (specialDate == null) {
                ps.setNull(5, java.sql.Types.DATE);
            } else {
                ps.setDate(5, specialDate);
            }

            ps.executeUpdate();
        } finally {
            pool.releaseConnection(pc);
        }
    }

    // =============================================================
    // 2. MANAGEMENT METHODS (staff UI)
    // =============================================================
    /**
     * Inserts a special opening-hours entry for a specific calendar date.
     * <p>
     * Overrides regular weekly hours for the given date.
     *
     * @param dateStr ISO date string (YYYY-MM-DD)
     * @param dayOfWeekIgnored ignored parameter (kept for compatibility)
     * @param open opening time (HH:mm)
     * @param close closing time (HH:mm)
     * @throws Exception if a database error occurs
     */
    public static synchronized void insertSpecialHour(String dateStr, String dayOfWeekIgnored, String open, String close) throws Exception {
        LocalDate date = LocalDate.parse(dateStr);
        String realDayName = date.getDayOfWeek().name();
        realDayName = realDayName.substring(0, 1) + realDayName.substring(1).toLowerCase();

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            // One special row per date: update it if it exists (adding the same date twice used to create
            // duplicates, and the lookup then picked an arbitrary one with LIMIT 1).
            try (PreparedStatement up = conn.prepareStatement(
                    "UPDATE opening_hours SET open_time=?, close_time=? WHERE is_special='YES' AND special_date=?")) {
                up.setString(1, open);
                up.setString(2, close);
                up.setDate(3, Date.valueOf(date));
                if (up.executeUpdate() > 0) return;
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO opening_hours (day_of_week, open_time, close_time, is_special, special_date) VALUES (?, ?, ?, 'YES', ?)")) {
                ps.setString(1, realDayName);
                ps.setString(2, open);
                ps.setString(3, close);
                ps.setDate(4, Date.valueOf(date));
                ps.executeUpdate();
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }

    /**
     * Loads one opening-hours row by id.
     *
     * @param id hours_id
     * @return the row, or null if not found
     * @throws Exception on database error
     */
    public static OpeningHoursDTO getById(int id) throws Exception {
        String sql = "SELECT * FROM opening_hours WHERE hours_id = ?";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                boolean isSpecial = "YES".equalsIgnoreCase(rs.getString("is_special"));
                return new OpeningHoursDTO(
                        rs.getInt("hours_id"),
                        rs.getString("day_of_week"),
                        rs.getString("open_time"),
                        rs.getString("close_time"),
                        isSpecial,
                        rs.getString("special_date"));
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }

    /**
     * Returns the regular weekly hours for a day name ("Monday"...), as {open, close} strings, or null.
     */
    public static String[] getWeeklyHours(String dayName) throws Exception {
        String sql = "SELECT open_time, close_time FROM opening_hours "
                + "WHERE (is_special IS NULL OR is_special='NO') AND day_of_week=? LIMIT 1";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dayName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new String[] { rs.getString(1), rs.getString(2) };
            }
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Retrieves all opening-hours entries.
     * <p>
     * Includes both regular weekly hours and special dates,
     * ordered for management display.
     *
     * @return list of opening-hours DTOs
     * @throws Exception if a database error occurs
     */
    public static List<OpeningHoursDTO> getAllOpeningHours() throws Exception {
        List<OpeningHoursDTO> list = new ArrayList<>();
        String sql = "SELECT * FROM opening_hours ORDER BY is_special ASC, special_date ASC, hours_id ASC";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                boolean isSpecial = "YES".equalsIgnoreCase(rs.getString("is_special"));
                String displayDay = isSpecial ? "Special" : rs.getString("day_of_week");
                list.add(new OpeningHoursDTO(
                    rs.getInt("hours_id"),
                    displayDay,
                    rs.getString("open_time"),
                    rs.getString("close_time"),
                    isSpecial,
                    rs.getString("special_date")
                ));
            }
        } finally {
            pool.releaseConnection(pc);
        }
        return list;
    }
    /**
     * Updates opening and closing times of an existing entry.
     *
     * @param id    opening-hours record ID
     * @param open  new opening time
     * @param close new closing time
     * @return true if update succeeded
     * @throws Exception if a database error occurs
     */
    public static boolean updateOpeningHour(int id, String open, String close) throws Exception {
        String sql = "UPDATE opening_hours SET open_time=?, close_time=? WHERE hours_id=?";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, open);
            ps.setString(2, close);
            ps.setInt(3, id);
            return ps.executeUpdate() > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Deletes an opening-hours entry by ID.
     *
     * @param id record identifier
     * @return true if deletion succeeded
     * @throws Exception if a database error occurs
     */
    public static boolean deleteOpeningHour(int id) throws Exception {
        String sql = "DELETE FROM opening_hours WHERE hours_id=?";
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            return ps.executeUpdate() > 0;
        } finally {
            pool.releaseConnection(pc);
        }
    }

    /** Returns String for UI Label (e.g. "08:00 - 22:00") */
    public static String getHoursForDate(LocalDate date) throws Exception {
        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();
        try {
            // Check Special
            String sqlSpecial = "SELECT open_time, close_time FROM opening_hours WHERE is_special='YES' AND special_date=?";
            try (PreparedStatement ps = conn.prepareStatement(sqlSpecial)) {
                ps.setDate(1, Date.valueOf(date));
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return formatRange(rs.getString(1), rs.getString(2)) + " (Special)";
                    }
                }
            }
            // Check Regular
            String dayName = date.getDayOfWeek().toString();
            dayName = dayName.substring(0, 1) + dayName.substring(1).toLowerCase();
            String sqlRegular = "SELECT open_time, close_time FROM opening_hours WHERE day_of_week=? AND (is_special IS NULL OR is_special='NO') LIMIT 1";
            try (PreparedStatement ps = conn.prepareStatement(sqlRegular)) {
                ps.setString(1, dayName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return formatRange(rs.getString(1), rs.getString(2));
                    }
                }
            }
            return "Closed";
        } finally {
            pool.releaseConnection(pc);
        }
    }
    
    private static String formatTime(String t) {
        if (t == null) return "";
        return (t.length() > 5) ? t.substring(0, 5) : t;
    }

    /** "10:00 - 22:00", or "Closed" when open == close (the way a closed day is stored). */
    private static String formatRange(String open, String close) {
        String o = formatTime(open);
        String c = formatTime(close);
        return o.equals(c) ? "Closed" : o + " - " + c;
    }

    // =============================================================
    // 3. LOGIC METHODS (reservation rules)
    // =============================================================
    /**
     * Checks whether a reservation can be scheduled at the given time.
     * <p>
     * Ensures the reservation fits entirely within opening hours.
     *
     * @param startTs        requested start time
     * @param diningMinutes duration of the visit
     * @return true if reservation is allowed
     * @throws Exception if a database error occurs
     */

    public static boolean isOpenForReservation(Timestamp startTs, int diningMinutes) throws Exception {
        if (startTs == null) return false;
        LocalDateTime start = startTs.toLocalDateTime();
        LocalDate date = start.toLocalDate();
        LocalTime time = start.toLocalTime();
        LocalDateTime end = start.plusMinutes(diningMinutes);
        LocalDate endDate = end.toLocalDate();
        LocalTime endTime = end.toLocalTime();

        if (!endDate.equals(date)) return false; // Must be same day

        OpenInterval interval = getOpenIntervalForDate(date);
        if (interval == null) return false;

        return !time.isBefore(interval.open) && !endTime.isAfter(interval.close);
    }

    /** Helper class for logic */
    private static class OpenInterval {
        final LocalTime open;
        final LocalTime close;
        OpenInterval(LocalTime open, LocalTime close) {
            this.open = open;
            this.close = close;
        }
    }

    private static OpenInterval getOpenIntervalForDate(LocalDate date) throws Exception {
        String sqlSpecial = "SELECT open_time, close_time FROM opening_hours WHERE is_special = 'YES' AND special_date = ? LIMIT 1";
        String sqlWeekly = "SELECT open_time, close_time FROM opening_hours WHERE (is_special IS NULL OR is_special = 'NO') AND day_of_week = ? LIMIT 1";

        MySQLConnectionPool pool = MySQLConnectionPool.getInstance();
        PooledConnection pc = pool.getConnection();
        Connection conn = pc.getConnection();

        try {
            // Check special
            try (PreparedStatement ps = conn.prepareStatement(sqlSpecial)) {
                ps.setDate(1, java.sql.Date.valueOf(date));
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Time o = rs.getTime("open_time");
                        Time c = rs.getTime("close_time");
                        if (o != null && c != null) return new OpenInterval(o.toLocalTime(), c.toLocalTime());
                    }
                }
            }
            // Check weekly
            String dowStr = date.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, Locale.ENGLISH);
            try (PreparedStatement ps = conn.prepareStatement(sqlWeekly)) {
                ps.setString(1, dowStr);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Time o = rs.getTime("open_time");
                        Time c = rs.getTime("close_time");
                        if (o != null && c != null) return new OpenInterval(o.toLocalTime(), c.toLocalTime());
                    }
                }
            }
            return null;
        } finally {
            pool.releaseConnection(pc);
        }
    }
    /**
     * Calculates available reservation time slots for a given date.
     * <p>
     * Considers opening hours, slot length, dining duration,
     * and same-day time restrictions.
     *
     * @param date           reservation date
     * @param slotMinutes    interval between slots
     * @param diningMinutes  duration of visit
     * @return list of available time slots (HH:mm)
     * @throws Exception if a database error occurs
     */
    public static List<String> getAvailableTimeSlots(LocalDate date, int slotMinutes, int diningMinutes) throws Exception {
        List<String> out = new ArrayList<>();
        if (date == null) return out;

        LocalDate today = LocalDate.now();
        if (date.isBefore(today) || date.isAfter(today.plusMonths(1))) return out;

        OpenInterval interval = getOpenIntervalForDate(date);
        if (interval == null) return out;

        LocalTime open = interval.open;
        LocalTime close = interval.close;

        // Closed day (open == close) or a window shorter than one meal -> no slots.
        // (close.minusMinutes() wraps around midnight, so 00:00 - 120 min used to give 22:00
        //  and a closed day returned a full list of slots.)
        int lastStartSec = close.toSecondOfDay() - diningMinutes * 60;
        if (!close.isAfter(open) || lastStartSec < open.toSecondOfDay()) return out;
        LocalTime lastStart = LocalTime.ofSecondOfDay(lastStartSec);

        LocalTime minStart = open;
        if (date.equals(today)) {
             java.time.LocalDateTime nowPlus1h = java.time.LocalDateTime.now().withSecond(0).withNano(0).plusHours(1);
             if (!nowPlus1h.toLocalDate().equals(today)) return out; // after 23:00: nothing left today
             if (nowPlus1h.toLocalTime().isAfter(minStart)) minStart = nowPlus1h.toLocalTime();
        }
        minStart = minStart.withSecond(0).withNano(0);

        int mod = minStart.getMinute() % slotMinutes;
        if (mod != 0) minStart = minStart.plusMinutes(slotMinutes - mod);

        for (LocalTime t = minStart; !t.isAfter(lastStart); t = t.plusMinutes(slotMinutes)) {
            out.add(String.format("%02d:%02d", t.getHour(), t.getMinute()));
        }
        return out;
    }
}