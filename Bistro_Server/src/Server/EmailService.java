package Server;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

/**
 * Utility service for sending system emails (reservation confirmations, reminders, waiting-list updates).
 * <p>
 * <b>Configuration</b> (first file that exists wins, all paths relative to the server's working directory):
 * <ol>
 *   <li>the file given by the JVM option {@code -Dbistro.email.config=/path/to/email.properties}</li>
 *   <li>{@code email.properties}</li>
 *   <li>{@code server.properties} (same file as the DB settings)</li>
 * </ol>
 * Required keys: {@code smtp.host}, {@code smtp.port}, {@code email.from}, {@code email.password}.
 * If nothing is configured, emails are skipped with one clear log line (the server keeps working).
 * <p>
 * <b>Sending is asynchronous</b>: emails are queued on one background thread, so a slow SMTP server never
 * blocks a client request or the background table-assignment job.
 */
public class EmailService {

    /** JVM option that points to an explicit config file. */
    private static final String CONFIG_PROPERTY = "bistro.email.config";

    /** Single daemon thread: keeps order, never blocks callers, never prevents JVM shutdown. */
    private static final ExecutorService MAILER = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "EmailSender");
        t.setDaemon(true);
        return t;
    });

    private static final Object CONFIG_LOCK = new Object();
    private static volatile boolean configLoaded = false;
    private static Session session;      // null => emails disabled
    private static String fromEmail;

    private EmailService() {}

    /**
     * Loads the SMTP configuration once (lazily) and builds the mail session.
     */
    private static void ensureConfig() {
        if (configLoaded) return;
        synchronized (CONFIG_LOCK) {
            if (configLoaded) return;

            Properties cfg = null;
            String usedFile = null;
            String explicit = System.getProperty(CONFIG_PROPERTY);
            String[] candidates = (explicit != null && !explicit.isBlank())
                    ? new String[] { explicit, "email.properties", "server.properties" }
                    : new String[] { "email.properties", "server.properties" };

            for (String file : candidates) {
                Properties p = tryLoad(file);
                if (p != null && p.getProperty("smtp.host") != null) {
                    cfg = p;
                    usedFile = file;
                    break;
                }
            }

            if (cfg == null) {
                System.out.println("[EMAIL] No SMTP configuration found (email.properties / server.properties). "
                        + "Emails are disabled; SMS stubs are still logged.");
                configLoaded = true;
                return;
            }

            String smtpHost = cfg.getProperty("smtp.host", "").trim();
            String smtpPort = cfg.getProperty("smtp.port", "").trim();
            String from = cfg.getProperty("email.from", "").trim();
            String appPassword = cfg.getProperty("email.password", "");

            if (smtpHost.isEmpty() || smtpPort.isEmpty() || from.isEmpty() || appPassword.isBlank()) {
                System.out.println("[EMAIL] " + usedFile + " is missing required keys "
                        + "(smtp.host, smtp.port, email.from, email.password). Emails are disabled.");
                configLoaded = true;
                return;
            }

            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", smtpHost);
            props.put("mail.smtp.port", smtpPort);
            props.put("mail.smtp.connectiontimeout", "10000");
            props.put("mail.smtp.timeout", "10000");

            final String user = from;
            session = Session.getInstance(props, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(user, appPassword);
                }
            });
            fromEmail = from;
            configLoaded = true;
            System.out.println("[EMAIL] SMTP configured from " + usedFile + " (" + smtpHost + ":" + smtpPort + ")");
        }
    }

    private static Properties tryLoad(String file) {
        try {
            Path path = Path.of(file);
            if (!Files.isRegularFile(path)) return null;
            Properties p = new Properties();
            try (InputStream in = new FileInputStream(path.toFile())) {
                p.load(in);
            }
            return p;
        } catch (Exception e) {
            System.out.println("[EMAIL] Could not read " + file + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Queues a plain-text email. Returns immediately; failures are logged by the sender thread.
     */
    private static void sendEmail(String toEmail, String subject, String body) {
        if (toEmail == null || toEmail.isBlank()) return;
        ensureConfig();
        if (session == null) return; // not configured

        MAILER.submit(() -> {
            try {
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(fromEmail, "Bistro System"));
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail));
                message.setSubject(subject);
                message.setText(body);
                Transport.send(message);
                System.out.println("[EMAIL] Sent to: " + toEmail + " | Subject: " + subject);
            } catch (Exception e) {
                System.out.println("[EMAIL] Failed to send to " + toEmail + " : " + e.getMessage());
            }
        });
    }

    /**
     * Sends a reservation confirmation email containing a confirmation code.
     *
     * @param toEmail recipient email
     * @param confirmationCode reservation confirmation code
     */
    public static void sendReservationConfirmation(String toEmail, String confirmationCode) {
        String subject = "Bistro Reservation Confirmation";
        String body = "Your reservation has been created successfully!\n\n"
                + "Confirmation Code: " + confirmationCode + "\n\n"
                + "Please keep this code to manage your reservation.\n";

        sendEmail(toEmail, subject, body);
    }

    /**
     * "I lost my code": re-sends the customer's active confirmation code.
     *
     * @param toEmail recipient email
     * @param code the confirmation code
     * @param reservation true for a reservation code, false for a waiting-list code
     */
    public static void sendLostCode(String toEmail, String code, boolean reservation) {
        String subject = "Bistro - Your confirmation code";
        String body =
                "Hello,\n\n" +
                "You asked for your " + (reservation ? "reservation" : "waiting list") + " confirmation code.\n\n" +
                "Confirmation code: " + code + "\n\n" +
                "Enter it at the restaurant terminal.\n\n" +
                "Bistro System";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends a reminder email 2 hours before a reservation time.
     *
     * @param toEmail recipient email
     * @param confirmationCode reservation confirmation code
     * @param timeStr reservation time as display string (e.g. "2026-01-20 18:30")
     */
    public static void sendReservationReminder(String toEmail, String confirmationCode, String timeStr) {
        String subject = "Bistro Reminder - Reservation in 2 hours";
        String body =
                "Hello,\n\n" +
                "This is a reminder that your reservation is in 2 hours.\n\n" +
                "Time: " + timeStr + "\n" +
                "Confirmation code: " + confirmationCode + "\n\n" +
                "See you soon,\n" +
                "Bistro System";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends an email confirming that the customer joined the waiting list, with their code.
     *
     * @param toEmail recipient email
     * @param waitingCode waiting-list confirmation code
     */
    public static void sendWaitingListJoined(String toEmail, String waitingCode) {
        String subject = "Bistro - You joined the waiting list";
        String body =
                "Hello,\n\n" +
                "You are on the Bistro waiting list.\n\n" +
                "Your waiting code: " + waitingCode + "\n\n" +
                "We will email you when a table is ready. Use this code at the terminal to check in,\n" +
                "or to leave the waiting list.\n\n" +
                "Bistro System";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends an email to a customer who asked for a table and was seated right away
     * (a table was free, so they never entered the waiting list).
     *
     * @param toEmail recipient email
     * @param code confirmation code (needed to pay the bill)
     * @param tableId the table to go to
     */
    public static void sendSeatedImmediately(String toEmail, String code, String tableId) {
        String subject = "Bistro - Your table is ready";
        String body =
                "Hello,\n\n" +
                "A table was free, so you were seated right away: table " + tableId + ".\n\n" +
                "Your confirmation code: " + code + "\n" +
                "Keep it - you will use it to pay the bill.\n\n" +
                "Enjoy your meal,\n" +
                "Bistro System";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends an email notifying a waiting-list guest that a table is ready.
     * The guest is expected to check-in within the hold window (15 minutes).
     *
     * @param toEmail recipient email
     * @param waitingCode waiting-list confirmation code
     */
    public static void sendWaitingTableReady(String toEmail, String waitingCode) {
        String subject = "Bistro - Your table is ready!";
        String body =
                "Hello,\n\n" +
                "A table is now available for you.\n\n" +
                "Please check in within 15 minutes using this code:\n" +
                waitingCode + "\n\n" +
                "See you soon,\n" +
                "Bistro System";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends the bill details 2 hours after the customer was seated (spec: "the customer receives a
     * message with the bill details"), with the code to pay it.
     *
     * @param toEmail recipient email
     * @param confirmationCode code used to pay
     * @param people number of diners
     * @param subtotal amount before discount
     * @param discount subscriber discount (0 for guests)
     * @param total amount to pay
     */
    public static void sendBillReminder(String toEmail, String confirmationCode,
                                        int people, double subtotal, double discount, double total) {
        String subject = "Bistro - Your bill";
        String body = """
            Hi,

            Your visit has reached the 2-hour limit. Here are your bill details:

              Diners:    %d
              Subtotal:  %.2f ILS
              Discount:  %.2f ILS%s
              Total:     %.2f ILS

            You can pay at the terminal or remotely with your confirmation code: %s

            Thank you,
            Bistro
            """.formatted(people, subtotal, discount, discount > 0 ? " (subscriber 10%)" : "", total, confirmationCode);

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends an email notifying a reservation guest that the assigned table is ready.
     *
     * @param toEmail recipient email
     * @param tableId assigned table identifier (e.g. "T12")
     */
    public static void sendReservationTableReady(String toEmail, String tableId) {
        String subject = "Bistro: Your table is ready";
        String body =
                "Hello,\n\n" +
                "Your table is ready: " + tableId + ".\n" +
                "Please go to the table now and scan your confirmation code again at the terminal within 15 minutes to confirm.\n\n" +
                "Thank you,\n" +
                "Bistro";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Temporary SMS stub for testing/logging (no real SMS integration).
     *
     * @param phone phone number
     * @param msg message text
     */
    public static void smsStub(String phone, String msg) {
        if (phone == null || phone.isBlank()) return;
        System.out.println("[SMS] phone=" + phone + " | " + msg);
    }

    /**
     * Sends an email notification to a customer regarding a reservation cancellation
     * caused by a change in the restaurant's opening hours.
     *
     * @param toEmail The recipient's email address.
     * @param date The date of the reservation (e.g., "2025-01-20" or "Every Monday").
     * @param reason The specific reason for cancellation (e.g., "Change in Opening Hours").
     */
    public static void sendCancellationNotification(String toEmail, String date, String reason) {
        String subject = "Important: Reservation Cancellation - Bistro Restaurant";
        String body = "Dear Customer,\n\n"
                + "We regret to inform you that your reservation for " + date + " has been canceled.\n"
                + "Reason: " + reason + "\n\n"
                + "Our opening hours have changed for this date. Please check our new schedule and book a different time.\n\n"
                + "We apologize for the inconvenience.\n"
                + "Sincerely,\nBistro Management";

        sendEmail(toEmail, subject, body);
    }

    /**
     * Sends a concise SMS notification (stub) about a reservation canceled due to schedule changes.
     *
     * @param phone The recipient's phone number.
     * @param date The date of the reservation.
     */
    public static void sendSMSCancellation(String phone, String date) {
        if (phone == null || phone.isBlank()) return;
        String msg = "Bistro Alert: Your reservation on " + date + " was canceled due to a change in opening hours. Please contact us.";
        smsStub(phone, msg);
    }
}
