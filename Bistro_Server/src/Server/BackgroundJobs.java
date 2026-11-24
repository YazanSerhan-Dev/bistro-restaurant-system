package Server;

import DataBase.dao.BillDAO;
import DataBase.dao.ReservationDAO;
import DataBase.dao.RestaurantTableDAO;
import DataBase.dao.WaitingListDAO;
import DataBase.Reservation;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * Manages all background and scheduled jobs of the Bistro server.
 *
 * This class is responsible for running periodic maintenance tasks such as:
 * <ul>
 *   <li>Canceling no-show reservations</li>
 *   <li>Handling waiting list assignments</li>
 *   <li>Releasing expired table reservations</li>
 *   <li>Sending reminder emails and SMS messages</li>
 *   <li>Generating monthly performance reports</li>
 * </ul>
 *
 * All jobs run in background threads using a {@link ScheduledExecutorService}.
 * The class is designed to start once per server lifecycle.
 */
public class BackgroundJobs {

    private static ScheduledExecutorService scheduler;
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final Object TABLE_ASSIGN_LOCK = new Object();

    // In-memory guard: reminder sent once per server run (reservation reminder only)
    private static final Set<Integer> reminderSent = ConcurrentHashMap.newKeySet();
    /** End of the last reminder window that was processed (null = first run). */
    private static volatile LocalDateTime lastReminderWindowEnd = null;
    /**
     * Starts all background jobs.
     *
     * This method initializes the scheduler and registers all periodic tasks.
     * If the jobs are already running, the method exits without effect.
     */
    public static void start() {

        // ✅ prevents duplicates
        if (!started.compareAndSet(false, true)) {
            System.out.println("[JOB] BackgroundJobs already running.");
            return;
        }

        // 5 periodic tasks. Daemon threads: jobs must never keep the JVM alive after the server exits.
        lastReminderWindowEnd = null;
        scheduler = Executors.newScheduledThreadPool(5, r -> {
            Thread t = new Thread(r, "BistroJob");
            t.setDaemon(true);
            return t;
        });

        // =========================
        // Thread #1: Cancel NO-SHOWS (15 minutes)
        // =========================
        scheduler.scheduleAtFixedRate(() -> {
            try {
                int updated = ReservationDAO.cancelNoShows15Min();
                if (updated > 0) {
                    System.out.println("[JOB] CANCELED no-shows (15 min): " + updated);
                }
            } catch (Exception e) {
                System.out.println("[JOB] cancelNoShows15Min error: " + e.getMessage());
            }
        }, 5, 30, TimeUnit.SECONDS);

     // =========================
     // Thread #2 (combined): Pending reservations FIRST, then waiting list
     // =========================
     scheduler.scheduleAtFixedRate(() -> {
         synchronized (TABLE_ASSIGN_LOCK) {
             try {
            	 

                 // (0) ✅ Release expired RESERVED holds for waiting list FIRST
                 int released = RestaurantTableDAO.releaseExpiredReservedTablesForWaiting();
                 if (released > 0) {
                     System.out.println("[JOB] Released expired RESERVED tables for waiting: " + released);
                 }

                 // (1) Waiting-list maintenance (also frees reserved tables by canceling ASSIGNED no-show)
                 int canceled = WaitingListDAO.cancelAssignedOver15Minutes();
                 if (canceled > 0) {
                     System.out.println("[JOB] CANCELED assigned waiting over 15min: " + canceled);
                 }

                 int oldCanceled = WaitingListDAO.cancelWaitingOlderThanHours(4);
                 if (oldCanceled > 0) {
                     System.out.println("[JOB] CANCELED old WAITING entries (4h): " + oldCanceled);
                 }

                 // (2) ✅ Reservations have priority (pending reserve)
                 ReservationDAO.autoReserveForPendingReservations();

                 int canceledPending = ReservationDAO.cancelPendingReservationsWithExpiredHold();
                 if (canceledPending > 0) {
                     System.out.println("[JOB] CANCELED pending reservations after reserved timeout: " + canceledPending);
                 }

                 // (3) Assign waiting list (after reservations had their chance)
                 int assignedCount = 0;

                 while (true) {
                     var next = WaitingListDAO.assignNextWaitingByReservingTable();
                     if (next == null) break;

                     assignedCount++;


                     String code = next.getConfirmationCode();

                     String email = WaitingListDAO.getEmailForWaitingId(next.getId());
                     String phone = WaitingListDAO.getPhoneForWaitingId(next.getId());

                     if (code != null && !code.isBlank()) {

                         if (email != null && !email.isBlank()) {
                             EmailService.sendWaitingTableReady(email, code);
                         }

                         if (phone != null && !phone.isBlank()) {
                             EmailService.smsStub(phone, "[SMS] Your table is ready! Waiting code: " + code);
                         }
                     }

                 }

                 if (assignedCount > 0) {
                     System.out.println("[JOB] ASSIGNED waiting entries: " + assignedCount);
                 }

             } catch (Exception e) {
                 System.out.println("[JOB] assignment cycle error: " + e.getMessage());
             }
         }
     }, 10, 20, TimeUnit.SECONDS);

        // =========================
        // Thread #4: Bill reminder after 2 hours (visit-based)
        // =========================
        scheduler.scheduleAtFixedRate(() -> {
            try {
                runBillReminderJobOnce();
            } catch (Exception e) {
                System.out.println("[JOB] Bill reminder job error: " + e.getMessage());
            }
        }, 30, 300, TimeUnit.SECONDS); // start after 30s, then every 5 min

        // =========================
        // OPTIONAL Thread #5: Reservation reminder (upcoming reservation in ~2 hours)
        // If you don't need it, you can delete this thread + method.
        // =========================
        scheduler.scheduleAtFixedRate(() -> {
            try {
                runUpcomingReservationReminderOnce();
            } catch (Exception e) {
                System.out.println("[JOB] reservation reminder error: " + e.getMessage());
            }
        }, 15, 60, TimeUnit.SECONDS);

        System.out.println("[JOB] BackgroundJobs started.");
        
        
        // =========================
        // Thread #6: Monthly Performance Report Generator
        // Runs once every 24 hours. Checks if there is data from last month to process.
        // =========================
        scheduler.scheduleAtFixedRate(() -> {
            try {
                int recordsCreated = DataBase.dao.PerformanceLogDAO.generateMonthlyReport();
                if (recordsCreated > 0) {
                    System.out.println("[JOB] Generated Monthly Performance Report. Rows added: " + recordsCreated);
                }
            } catch (Exception e) {
                System.out.println("[JOB] Monthly Report Error: " + e.getMessage());
            }
        }, 0, 24, TimeUnit.HOURS); // Start immediately, repeat every 24 hours
    }
    /**
     * Stops all background jobs immediately.
     *
     * Shuts down the scheduler and prevents further execution of tasks.
     */
    public static void stop() {

        if (!started.compareAndSet(true, false)) {
            System.out.println("[JOB] BackgroundJobs not running.");
            return;
        }

        try {
            if (scheduler != null) scheduler.shutdownNow();
        } finally {
            scheduler = null;
        }

        System.out.println("[JOB] BackgroundJobs stopped.");
    }

    // =========================================================
    // Bill reminder logic (visit-based) - runs ONCE each time thread triggers
    // =========================================================
    /**
     * Sends payment reminders for visits that exceeded the allowed time.
     *
     * This method is triggered periodically and runs once per execution cycle.
     * A reminder is sent only if it was not already sent before.
     */
    private static void runBillReminderJobOnce() throws Exception {

        // Parties seated 2+ hours ago may not have a bill yet (bills are otherwise created on "Find Bill").
        int created = BillDAO.createMissingBillsForOverdueVisits();
        if (created > 0) {
            System.out.println("[JOB] Created " + created + " bill(s) for parties seated over 2 hours");
        }

        List<BillDAO.BillReminderRow> due = BillDAO.findBillsNeedingReminder(25);

        for (BillDAO.BillReminderRow row : due) {
            try {
                // Bill details by email + SMS (email skipped if the customer has none)
                EmailService.sendBillReminder(row.email, row.confirmationCode,
                        row.people, row.subtotal, row.discount, row.total);
                EmailService.smsStub(row.phone, String.format(
                        "Your Bistro bill: %d diners, subtotal %.2f, discount %.2f, total %.2f ILS. Pay with code %s",
                        row.people, row.subtotal, row.discount, row.total, row.confirmationCode));
                BillDAO.markReminderSent(row.billId);

                System.out.println("[JOB] Bill reminder queued for billId=" + row.billId +
                        " | code=" + row.confirmationCode);

            } catch (Exception sendErr) {
                System.out.println("[JOB] Bill reminder failed for billId=" + row.billId +
                        " : " + sendErr.getMessage());
            }
        }
    }

    // =========================================================
    //  Upcoming reservation reminder (~2 hours before reservation_time)
    // =========================================================
    /**
     * Sends reminder emails for upcoming reservations.
     *
     * A reminder is sent approximately two hours before the reservation time.
     * Each reservation is reminded only once per server runtime.
     */
    private static void runUpcomingReservationReminderOnce() throws Exception {

        // Consecutive, non-overlapping windows (previousEnd, now+2h]: every reservation falls into
        // exactly one run, so a reminder is not sent twice even if the server restarts
        // (the in-memory set below is only an extra guard within one run).
        LocalDateTime to = LocalDateTime.now().plusHours(2);
        LocalDateTime from = (lastReminderWindowEnd != null) ? lastReminderWindowEnd : to.minusMinutes(1);
        if (!to.isAfter(from)) return;

        // Only the CONFIRMED reservations in the window (no longer loads the whole table every minute)
        List<Reservation> due = ReservationDAO.getConfirmedReservationsStartingBetween(
                Timestamp.valueOf(from), Timestamp.valueOf(to));
        lastReminderWindowEnd = to;

        for (Reservation r : due) {

            if (!reminderSent.add(r.getReservationId())) continue; // already reminded in this run

            LocalDateTime resTime = r.getReservationTime().toLocalDateTime();

            try {
                String email = ReservationDAO.getReservationEmail(r.getReservationId());
                if (email != null && !email.isBlank()) {

                    String timeStr = resTime.toString().replace('T', ' ');

                    EmailService.sendReservationReminder(
                            email,
                            r.getConfirmationCode(),
                            timeStr
                    );

                    System.out.println("[REMINDER] Reservation reminder email sent to " + email +
                            " for reservation " + r.getReservationId());
                }

                // SMS stub (optional)
                String phone = ReservationDAO.getReservationPhone(r.getReservationId());
                EmailService.smsStub(phone, "Reservation reminder in 2 hours | Code: " + r.getConfirmationCode());

            } catch (Exception e) {
                reminderSent.remove(r.getReservationId()); // allow a retry if lookup failed
                System.out.println("[REMINDER] Failed for reservation " +
                        r.getReservationId() + ": " + e.getMessage());
            }
        }
    }
    
    

}


