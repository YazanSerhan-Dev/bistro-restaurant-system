package Server;

import java.io.IOException;
import java.io.ObjectInputFilter;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ocsf.server.AbstractServer;
import ocsf.server.ConnectionToClient;

import common.Envelope;
import common.KryoMessage;
import common.KryoUtil;
import common.OpCode;
import common.dto.LoginRequestDTO;
import common.dto.LoginResponseDTO;
import common.dto.MakeReservationRequestDTO;
import common.dto.MakeReservationResponseDTO;
import common.dto.OpeningHoursDTO;
import common.dto.ProfileDTO;
import common.dto.RegistrationDTO;
import common.dto.ReservationDTO;
import common.dto.RestaurantTableDTO;
import common.dto.SubscriberDTO;
import common.dto.TerminalActiveItemDTO;
import common.dto.TerminalValidateResponseDTO;
import common.dto.WaitingListDTO;

import DataBase.Reservation;
import DataBase.dao.BillDAO;
import DataBase.dao.OpeningHoursDAO;
import DataBase.dao.ReservationDAO;
import DataBase.dao.RestaurantTableDAO;
import DataBase.dao.StaffDAO;
import DataBase.dao.SubscriberDAO;
import DataBase.dao.UserActivityDAO;
import DataBase.dao.VisitDAO;
import DataBase.dao.WaitingListDAO;
/**
 * Main OCSF server of the Bistro system.
 * <p>
 * Responsibilities:
 * </p>
 * <ul>
 *   <li>Starts/stops server and background jobs.</li>
 *   <li>Receives client requests (as {@link Envelope}) and routes them by {@link OpCode}.</li>
 *   <li>Performs business validations and calls DAO layer.</li>
 *   <li>Sends responses back using {@link KryoMessage} serialization.</li>
 * </ul>
 *
 */
public class BistroServer extends AbstractServer {
	 /** UI controller for the server window (may be null when running headless). */
    private final ServerController controller;
    /** DAO for reservation operations (non-static methods exist here). */
    private final ReservationDAO reservationDAO = new ReservationDAO();

    private MqttBridge mqtt;
    private static final String MQTT_TOPIC = "bistro/reservations/today";
    private ReservationStatsMqttPublisher mqttPublisher;

    // ==================== Per-connection session ====================
    // The server never trusts the role/username a client puts in a payload.
    // Identity is stored on the connection after a successful login (or QR scan).
    private static final String S_ROLE = "session.role";
    private static final String S_USER = "session.username";
    private static final String S_MEMBER = "session.memberCode";

    /** Operations that require a logged-in staff member (AGENT or MANAGER). */
    private static final Set<OpCode> STAFF_OPS = EnumSet.of(
            OpCode.REQUEST_REGISTER_CUSTOMER,
            OpCode.REQUEST_SUBSCRIBERS_LIST,
            OpCode.REQUEST_AGENT_RESERVATIONS_LIST,
            OpCode.REQUEST_WAITING_LIST,
            OpCode.REQUEST_WAITING_REMOVE,
            OpCode.REQUEST_CURRENT_DINERS,
            OpCode.REQUEST_TABLES_GET,
            OpCode.REQUEST_TABLE_ADD,
            OpCode.REQUEST_TABLE_REMOVE,
            OpCode.REQUEST_TABLE_UPDATE,
            OpCode.REQUEST_OPENING_HOURS_GET,
            OpCode.REQUEST_OPENING_HOURS_UPDATE,
            OpCode.REQUEST_OPENING_HOURS_ADD_SPECIAL,
            OpCode.REQUEST_OPENING_HOURS_REMOVE);

    /** Operations that require a logged-in manager. */
    private static final Set<OpCode> MANAGER_OPS = EnumSet.of(
            OpCode.REQUEST_REPORT_PERFORMANCE,
            OpCode.REQUEST_REPORT_ACTIVITY);

    /** Operations that require an identified subscriber (login or QR scan). */
    private static final Set<OpCode> SUBSCRIBER_OPS = EnumSet.of(
            OpCode.REQUEST_GET_PROFILE,
            OpCode.REQUEST_UPDATE_PROFILE,
            OpCode.REQUEST_TERMINAL_GET_SUBSCRIBER_ACTIVE_CODES);

    private static String sessionRole(ConnectionToClient c) {
        Object r = c.getInfo(S_ROLE);
        return r == null ? "" : r.toString();
    }

    private static String sessionUser(ConnectionToClient c) {
        Object u = c.getInfo(S_USER);
        return u == null ? "" : u.toString();
    }

    private static String sessionMemberCode(ConnectionToClient c) {
        Object m = c.getInfo(S_MEMBER);
        return m == null ? "" : m.toString();
    }

    private static boolean isStaff(ConnectionToClient c) {
        String r = sessionRole(c);
        return "AGENT".equalsIgnoreCase(r) || "MANAGER".equalsIgnoreCase(r);
    }

    private static boolean isManager(ConnectionToClient c) {
        return "MANAGER".equalsIgnoreCase(sessionRole(c));
    }

    private static boolean isSubscriber(ConnectionToClient c) {
        return "SUBSCRIBER".equalsIgnoreCase(sessionRole(c)) && !sessionUser(c).isBlank();
    }

    private static void setSession(ConnectionToClient c, String role, String username, String memberCode) {
        c.setInfo(S_ROLE, role);
        c.setInfo(S_USER, username);
        c.setInfo(S_MEMBER, memberCode);
    }

    private static void clearSession(ConnectionToClient c) {
        setSession(c, null, null, null);
    }

    /**
     * Turns an exception into a message that is safe to show to a client.
     * Database errors are logged on the server and replaced with a generic text,
     * so SQL details are never leaked.
     */
    private String safeMsg(Exception e) {
        if (e instanceof SQLException || e.getCause() instanceof SQLException) {
            log("Database error: " + e.getMessage());
            return "database error, please try again.";
        }
        return e.getMessage() == null ? "unexpected error." : e.getMessage();
    }

    /** Shared slot rules: 30-minute slots, 2-hour dining window. */
    private static final int SLOT_MINUTES = 30;
    private static final int DINING_MINUTES = 120;


    /*
     * OCSF reads plain Java-serialized objects from every client socket. Without a filter, a malicious
     * client could send any Serializable class (a classic Java deserialization attack). Clients only
     * ever send KryoMessage(String, byte[]), so everything else is rejected.
     * <p>
     * Only per-object limits are used (maxdepth, maxarray). "maxrefs"/"maxbytes" must NOT be used:
     * Java counts them over the whole lifetime of the stream, so a long-lived client connection was
     * dropped after a few hundred requests (found by the load test).
     */
    static {
        try {
            ObjectInputFilter.Config.setSerialFilter(ObjectInputFilter.Config.createFilter(
                    "maxdepth=5;maxarray=10485760;"
                    + "common.KryoMessage;java.lang.String;!*"));
        } catch (IllegalStateException alreadySet) {
            // a filter was already installed for this JVM
        }
    }

    /**
     * Creates a new server instance.
     *
     * @param port       listening port
     * @param controller JavaFX controller used to update UI/logs (can be null)
     */
    public BistroServer(int port, ServerController controller) {
        super(port);
        this.controller = controller;

        // OCSF's default listen backlog is 10: a burst of connections beyond that was refused by the OS
        // (load test: only 24 of 100 simultaneous clients could connect). Let bursts queue instead.
        setBacklog(ServerConfig.getInt("server.backlog", 200));

        // server.workers = 0 keeps the original OCSF behaviour (one request at a time, inside the lock);
        // used by the load test to measure "before" and "after" with the same code.
        int threads = ServerConfig.getInt("server.workers",
                Math.max(8, Math.min(32, Runtime.getRuntime().availableProcessors() * 2)));
        this.workers = (threads <= 0) ? null : Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "bistro-worker");
            t.setDaemon(true);
            return t;
        });
    }

    // ==================== Parallel request handling ====================
    //
    // OCSF calls handleMessageFromClient() from a *synchronized* method, so the server used to process
    // ONE request at a time for all clients (load test: flat ~350 req/s, latency growing with every
    // client). Now handleMessageFromClient() only queues the request and returns at once; a worker pool
    // runs requests from different clients in parallel, while each client's own requests still run
    // one after another in the order they were sent (same behaviour the client code relies on).

    /** Worker threads that run the request handlers (null = legacy single-threaded mode). */
    private final ExecutorService workers;

    private static final String S_QUEUE = "session.queue";

    /** Runs one client's requests in order, on the shared worker pool. */
    private static final class ClientQueue {
        private final ExecutorService pool;
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean running = false;

        ClientQueue(ExecutorService pool) { this.pool = pool; }

        synchronized void submit(Runnable task) {
            tasks.add(task);
            if (!running) {
                running = true;
                pool.execute(this::drain);
            }
        }

        private void drain() {
            while (true) {
                Runnable next;
                synchronized (this) {
                    next = tasks.poll();
                    if (next == null) { running = false; return; }
                }
                try {
                    next.run();
                } catch (Throwable t) {
                    System.out.println("[WORKER] Unhandled error: " + t);
                }
            }
        }
    }

    /**
     * Called by OCSF (inside its global lock): only hand the request to this client's queue.
     */
    @Override
    protected void handleMessageFromClient(Object msg, ConnectionToClient client) {
        ClientQueue q = (ClientQueue) client.getInfo(S_QUEUE);
        if (workers == null || q == null || workers.isShutdown()) {
            processMessage(msg, client); // legacy mode or shutting down: handle inline
            return;
        }
        try {
            q.submit(() -> processMessage(msg, client));
        } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
            // pool closed during shutdown: drop the request
        }
    }

    /** Stops the worker pool when the server is closed. */
    @Override
    protected void serverClosed() {
        if (workers != null) workers.shutdown();
    }
    /**
     * Writes a log line either to the UI (if available) or to console.
     *
     * @param text message to log
     */
    private void log(String text) {
        if (controller != null) controller.appendLogFromServer(text);
        else System.out.println(text);
    }
    /**
     * Extracts host name from client connection.
     *
     * @param c client connection
     * @return host name or "unknown" if missing
     */
    private String host(ConnectionToClient c) {
        return (c.getInetAddress() != null) ? c.getInetAddress().getHostName() : "unknown";
    }
    /**
     * Extracts IP address from client connection.
     *
     * @param c client connection
     * @return ip address or "unknown" if missing
     */
    private String ip(ConnectionToClient c) {
        return (c.getInetAddress() != null) ? c.getInetAddress().getHostAddress() : "unknown";
    }
    /**
     * Called by OCSF when server starts listening.
     * Starts background jobs and updates UI.
     */
    @Override
    protected void serverStarted() {
        log("Server started on port " + getPort());
        if (controller != null) controller.onServerStarted(getPort());
        BackgroundJobs.start();

        if (!ServerConfig.getBoolean("mqtt.enabled", true)) {
            log("[MQTT] Disabled (mqtt.enabled=false).");
            return;
        }

        String brokerUrl = ServerConfig.get("mqtt.url", "tcp://localhost:1883");
        String clientId = ServerConfig.get("mqtt.clientId", "bistro-server");
        mqtt = new MqttBridge(brokerUrl, clientId);
        try {
            mqtt.connect();
        } catch (Exception e) {
            // Not fatal: the publisher keeps retrying in the background
            log("[MQTT] Broker not reachable at " + brokerUrl + " (" + e.getMessage() + "), will retry.");
        }
        mqttPublisher = new ReservationStatsMqttPublisher(mqtt, MQTT_TOPIC, 2000);
        mqttPublisher.start();
        log("[MQTT] Stats publisher started -> " + brokerUrl);
    }

    /**
     * Called by OCSF when server is stopped.
     * Stops background jobs and MQTT, and updates UI.
     */
    @Override
    protected void serverStopped() {
        log("Server stopped.");
        if (controller != null) controller.onServerStopped();
        BackgroundJobs.stop();

        try {
            if (mqttPublisher != null) mqttPublisher.stop();
        } catch (Exception ignored) {}
        try {
            if (mqtt != null) mqtt.close(); // disconnect so Paho threads don't keep the JVM alive
        } catch (Exception ignored) {}
        mqttPublisher = null;
        mqtt = null;
    }

    /**
     * Called when a client connects.
     * Stores host/ip as client info and updates UI.
     *
     * @param client client that connected
     */
    @Override
    protected void clientConnected(ConnectionToClient client) {
        super.clientConnected(client);
        String ip = ip(client);

        // All per-connection keys are created here, once, before any worker touches them
        // (later setInfo() calls only replace values, they never change the map's structure).
        client.setInfo("ip", ip);
        client.setInfo("host", ip);
        setSession(client, null, null, null);

        // The host name needs a reverse-DNS lookup, which can take seconds on a LAN. This method runs
        // while OCSF holds its global lock, so the lookup is done on a worker thread instead.
        Runnable announce = () -> {
            String host = host(client);
            client.setInfo("host", host);
            log("Client connected: " + host + " (" + ip + ")");
            if (controller != null) controller.onClientConnected(host, ip);
        };

        if (workers == null) { // legacy single-threaded mode
            announce.run();
            return;
        }
        client.setInfo(S_QUEUE, new ClientQueue(workers));
        try {
            workers.execute(announce);
        } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
            // server is closing
        }
    }
    /**
     * Called when a client disconnects normally.
     *
     * @param client client that disconnected
     */
    @Override
    protected void clientDisconnected(ConnectionToClient client) {
        super.clientDisconnected(client);
        String host = (String) client.getInfo("host");
        String ip = (String) client.getInfo("ip");
        if (host == null) host = host(client);
        if (ip == null) ip = ip(client);
        log("Client disconnected: " + host + " (" + ip + ")");
        if (controller != null) controller.onClientDisconnected(host, ip);
    }

    /**
     * Called when client disconnects due to an exception.
     *
     * @param client    client connection
     * @param exception thrown error
     */
    @Override
    protected void clientException(ConnectionToClient client, Throwable exception) {
        super.clientException(client, exception);
        String host = (String) client.getInfo("host");
        String ip = (String) client.getInfo("ip");
        if (host == null) host = host(client);
        if (ip == null) ip = ip(client);
        log("Client exception: " + host + " (" + ip + "): " + exception.getMessage());
        if (controller != null) controller.onClientDisconnected(host, ip);
    }

    /**
     * Main message handler (runs on a worker thread). Decodes the request into {@link Envelope},
     * checks authorization and routes it by {@link OpCode}.
     *
     * @param msg    incoming object from client (usually {@link KryoMessage} or {@link Envelope})
     * @param client client connection
     */
    private void processMessage(Object msg, ConnectionToClient client) {
        try {
            Envelope req = unwrapToEnvelope(msg);
            if (req == null || req.getOp() == null) {
                sendError(client, OpCode.ERROR, "Bad message format (expected Envelope via KryoMessage).");
                return;
            }

            OpCode op = req.getOp();

            // ---- Authorization: decided by the server-side session, never by the payload ----
            if (MANAGER_OPS.contains(op) && !isManager(client)) {
                sendError(client, OpCode.ERROR, "Access denied: manager login required.");
                return;
            }
            if (STAFF_OPS.contains(op) && !isStaff(client)) {
                sendError(client, OpCode.ERROR, "Access denied: staff login required.");
                return;
            }
            if (SUBSCRIBER_OPS.contains(op) && !isSubscriber(client)) {
                sendError(client, OpCode.ERROR, "Access denied: subscriber login required.");
                return;
            }

            switch (op) {
                // --- SESSION ---
                case REQUEST_LOGIN_SUBSCRIBER -> handleLoginSubscriber(req, client);
                case REQUEST_LOGIN_STAFF      -> handleLoginStaff(req, client);
                case REQUEST_LOGOUT           -> clearSession(client); // no reply needed

                // --- RESERVATIONS ---
                case REQUEST_RESERVATIONS_LIST -> handleReservationsList(req, client);
                case REQUEST_MAKE_RESERVATION -> handleMakeReservation(req, client);
                case REQUEST_CHECK_AVAILABILITY -> handleCheckAvailability(req, client);
                case REQUEST_CANCEL_RESERVATION -> handleCancelReservation(req, client);
                case REQUEST_GET_AVAILABLE_TIMES -> handleGetAvailableTimes(req, client);
                case REQUEST_RECOVER_CONFIRMATION_CODE -> handleRecoverConfirmationCode(req, client);

                // --- TERMINAL ---
                case REQUEST_TERMINAL_VALIDATE_CODE -> handleTerminalValidateCode(req, client);
                case REQUEST_TERMINAL_CHECK_IN -> handleTerminalCheckIn(req, client);
                case REQUEST_TERMINAL_CANCEL_RESERVATION -> handleTerminalCancelReservation(req, client);
                case REQUEST_TERMINAL_GET_SUBSCRIBER_ACTIVE_CODES -> handleTerminalGetSubscriberActiveCodes(client);
                case REQUEST_TERMINAL_RESOLVE_SUBSCRIBER_QR -> handleTerminalResolveSubscriberQR(req, client);

                // --- WAITING LIST ---
                case REQUEST_WAITING_LIST -> handleGetWaitingList(client);          // staff view
                case REQUEST_WAITING_ADD  -> handleJoinWaitingList(req, client);    // terminal / staff walk-in
                case REQUEST_WAITING_REMOVE -> handleRemoveWaitingCustomer(req, client);
                case REQUEST_LEAVE_WAITING_LIST -> handleLeaveWaitingList(req, client);

                // --- STAFF DASHBOARD ---
                case REQUEST_REGISTER_CUSTOMER -> handleRegisterCustomer(req, client);
                case REQUEST_SUBSCRIBERS_LIST -> handleSubscribersList(client);
                case REQUEST_AGENT_RESERVATIONS_LIST -> handleAgentReservationsList(client);
                case REQUEST_SUBSCRIBER_HISTORY -> handleSubscriberHistory(req, client);
                case REQUEST_CURRENT_DINERS -> handleCurrentDiners(client);

                case REQUEST_TABLES_GET -> handleGetTables(client);
                case REQUEST_TABLE_ADD -> handleAddTable(req, client);
                case REQUEST_TABLE_REMOVE -> handleRemoveTable(req, client);
                case REQUEST_TABLE_UPDATE -> handleUpdateTable(req, client);

                case REQUEST_OPENING_HOURS_GET -> handleGetOpeningHours(client);
                case REQUEST_OPENING_HOURS_UPDATE -> handleUpdateOpeningHours(req, client);
                case REQUEST_OPENING_HOURS_ADD_SPECIAL -> handleAddSpecialHour(req, client);
                case REQUEST_OPENING_HOURS_REMOVE -> handleRemoveSpecialHour(req, client);
                case REQUEST_TODAY_HOURS -> handleGetTodayHours(client);

                case REQUEST_REPORT_PERFORMANCE -> handleReportPerformance(req, client);
                case REQUEST_REPORT_ACTIVITY -> handleReportActivity(req, client);

                // --- BILLING / PROFILE ---
                case REQUEST_BILL_GET_BY_CODE -> handleBillGetByCode(req, client);
                case REQUEST_PAY_BILL        -> handlePayBill(req, client);
                case REQUEST_GET_PROFILE -> handleGetProfile(client);
                case REQUEST_UPDATE_PROFILE -> handleUpdateProfile(req, client);

                default -> sendError(client, OpCode.ERROR, "Unknown op: " + op);
            }

        } catch (Exception e) {
            log("Error handling message: " + e.getMessage());
            try {
                sendError(client, OpCode.ERROR, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Converts incoming object into {@link Envelope}.
     * Supports direct {@link Envelope} or {@link KryoMessage} that contains an Envelope payload.
     *
     * @param msg received message object
     * @return envelope or null if cannot decode
     */
    private Envelope unwrapToEnvelope(Object msg) {
        try {
            if (msg instanceof Envelope e) return e;

            if (msg instanceof KryoMessage km) {
                Object obj = KryoUtil.fromBytes(km.getPayload());
                if (obj instanceof Envelope e) return e;
            }
        } catch (Exception ex) {
            log("Decode error: " + ex.getMessage());
        }
        return null;
    }

    /* ==================== Envelope replies ==================== */
    /**
     * Sends a successful response.
     *
     * @param client  target client
     * @param op      response opcode
     * @param payload response payload object
     * @throws IOException if send fails
     */
    private void sendOk(ConnectionToClient client, OpCode op, Object payload) throws IOException {
        Envelope resp = Envelope.ok(op, payload);
        sendEnvelope(client, resp);
    }
    /**
     * Sends an error response.
     *
     * @param client  target client
     * @param op      response opcode (usually ERROR or specific response)
     * @param message human readable error message
     * @throws IOException if send fails
     */
    private void sendError(ConnectionToClient client, OpCode op, String message) throws IOException {
        Envelope resp = Envelope.error(message);
        resp.setOp(op);
        sendEnvelope(client, resp);
    }

    /**
     * Serializes envelope via Kryo and sends it.
     *
     * @param client target client
     * @param env    envelope to send
     * @throws IOException if sending fails
     */
    private void sendEnvelope(ConnectionToClient client, Envelope env) throws IOException {
        byte[] bytes = KryoUtil.toBytes(env);
        client.sendToClient(new KryoMessage("ENVELOPE", bytes));
    }

    /* ==================== Handlers ==================== */
    /**
     * Returns active reservation/waiting codes for a subscriber username (terminal screen use).
     */
    private void handleTerminalGetSubscriberActiveCodes(ConnectionToClient client) {
        try {
            // Always the identified subscriber of this connection (never a username from the payload)
            String username = sessionUser(client);

            var items = UserActivityDAO.listActiveItemsBySubscriberUsername(username);

            sendOk(client,
                OpCode.RESPONSE_TERMINAL_GET_SUBSCRIBER_ACTIVE_CODES,
                items == null ? new ArrayList<>() : items);

        } catch (Exception e) {
            try {
                sendOk(client,
                    OpCode.RESPONSE_TERMINAL_GET_SUBSCRIBER_ACTIVE_CODES,
                    new ArrayList<>());
            } catch (Exception ignored) {}
        }
    }

    /**
     * Resolves subscriber QR (barcode) into username and list of active terminal items.
     * Response payload is Object[] { username, items } or an error string.
     */
    private void handleTerminalResolveSubscriberQR(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String barcode = (payload instanceof String s) ? s.trim() : "";

            if (barcode.isBlank()) {
                sendOk(client,
                    OpCode.RESPONSE_TERMINAL_RESOLVE_SUBSCRIBER_QR,
                    "Invalid QR value.");
                return;
            }

            // 1) QR decides if this is a subscriber
            String username = SubscriberDAO.getUsernameByBarcodeData(barcode);

            if (username == null || username.isBlank()) {
                sendOk(client,
                    OpCode.RESPONSE_TERMINAL_RESOLVE_SUBSCRIBER_QR,
                    "This QR does not belong to a subscriber.");
                return;
            }

            // The QR card identifies the subscriber on this terminal connection
            String memberCode = SubscriberDAO.getMemberCodeByUsername(username);
            setSession(client, "SUBSCRIBER", username, memberCode == null ? "" : memberCode.trim());

            // 2) Subscriber found -> fetch ALL active items
            List<TerminalActiveItemDTO> items =
                UserActivityDAO.listActiveItemsBySubscriberUsername(username);

            if (items == null) items = new ArrayList<>();
            else items = new ArrayList<>(items);

            String email = SubscriberDAO.getEmailByUsername(username);
            String phone = SubscriberDAO.getPhoneByUsername(username);

            if (email == null) email = "";
            if (phone == null) phone = "";

            sendOk(client,
                OpCode.RESPONSE_TERMINAL_RESOLVE_SUBSCRIBER_QR,
                new Object[]{ username, email, phone, items }
            );


        } catch (Exception e) {
            try {
                sendOk(client,
                    OpCode.RESPONSE_TERMINAL_RESOLVE_SUBSCRIBER_QR,
                    "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Returns available reservation time slots for a given date string (YYYY-MM-DD).
     */
    
    private void handleGetAvailableTimes(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();

            // payload: Object[] { "YYYY-MM-DD", partySize }  (or just the date String, for old clients)
            String dateStr = "";
            int people = 0;
            if (payload instanceof Object[] arr && arr.length >= 1) {
                dateStr = arr[0] == null ? "" : arr[0].toString().trim();
                if (arr.length >= 2 && arr[1] instanceof Number n) people = n.intValue();
            } else if (payload instanceof String s) {
                dateStr = s.trim();
            }
            if (dateStr.isBlank()) {
                sendOk(client, OpCode.RESPONSE_GET_AVAILABLE_TIMES, new ArrayList<>());
                return;
            }

            LocalDate date;
            try {
                date = LocalDate.parse(dateStr);
            } catch (Exception ex) {
                sendOk(client, OpCode.RESPONSE_GET_AVAILABLE_TIMES, new ArrayList<>());
                return;
            }

            // Hours when the restaurant is open (30-minute slots, 2-hour stay, 1 hour notice, 1 month max)
            List<String> times = OpeningHoursDAO.getAvailableTimeSlots(date, SLOT_MINUTES, DINING_MINUTES);

            // Spec: show ONLY times when the restaurant is open AND there is room for this party
            if (people > 0) {
                times = (partyTooLargeMessage(people) != null)
                        ? new ArrayList<>()
                        : RestaurantTableDAO.filterSlotsWithRoom(date, times, people, DINING_MINUTES);
            }

            sendOk(client, OpCode.RESPONSE_GET_AVAILABLE_TIMES, new ArrayList<>(times));

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_GET_AVAILABLE_TIMES, new ArrayList<>());
            } catch (Exception ignored) {}
        }
    }
    /**
     * Returns subscriber reservation history + visits list.
     * <ul>
     *   <li>Staff: history of the subscriber named in the payload.</li>
     *   <li>Subscriber: ALWAYS their own history (spec: a subscriber can see their reservations and
     *       visits history); the payload is ignored.</li>
     * </ul>
     */
    private void handleSubscriberHistory(Envelope req, ConnectionToClient client) {
        try {
            String username;
            if (isStaff(client)) {
                username = (req.getPayload() instanceof String s) ? s.trim() : null;
            } else if (isSubscriber(client)) {
                username = sessionUser(client);
            } else {
                sendError(client, OpCode.RESPONSE_SUBSCRIBER_HISTORY, "Access denied: please log in.");
                return;
            }

            if (username == null || username.isEmpty()) {
                // Send empty history if no username
                sendOk(client, OpCode.RESPONSE_SUBSCRIBER_HISTORY, 
                       new common.dto.HistoryDTO(new ArrayList<>(), new ArrayList<>()));
                return;
            }

            // 1. Fetch Reservations (Using the JOIN query)
            List<ReservationDTO> reservations = ReservationDAO.getReservationsBySubscriberForStaff(username);

            // 2. Fetch Visits (Using the JOIN query)
            List<String> visits = VisitDAO.getVisitsBySubscriber(username);

            // 3. Send Response
            common.dto.HistoryDTO history = new common.dto.HistoryDTO(reservations, visits);
            sendOk(client, OpCode.RESPONSE_SUBSCRIBER_HISTORY, history);

        } catch (Exception e) {
            log("Error fetching history: " + e.getMessage());
            try {
                sendError(client, OpCode.RESPONSE_SUBSCRIBER_HISTORY, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Cancels reservation by confirmation code (terminal cancel flow).
     */
    private void handleTerminalCancelReservation(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String code = (payload instanceof String s) ? s.trim() : "";

            if (code.isBlank()) {
                sendOk(client, OpCode.RESPONSE_TERMINAL_CANCEL_RESERVATION, "Missing confirmation code.");
                return;
            }

            ReservationDAO.CancelByCodeResult r = ReservationDAO.cancelReservationByCode(code);

            sendOk(client, OpCode.RESPONSE_TERMINAL_CANCEL_RESERVATION, r.message);

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_TERMINAL_CANCEL_RESERVATION,
                        "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Recovers an active confirmation code by email/phone (lost-code flow).
     * Sends the code by email (and SMS stub).
     */
    private void handleRecoverConfirmationCode(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String contact = (payload instanceof String s) ? s.trim() : "";

            if (contact.isBlank()) {
                sendOk(client, OpCode.RESPONSE_RECOVER_CONFIRMATION_CODE,
                        "Email or phone is required.");
                return;
            }

            UserActivityDAO.LostCodeResult r =
                    UserActivityDAO.findActiveCodeByContact(contact);

            if (r == null) {
                sendOk(client, OpCode.RESPONSE_RECOVER_CONFIRMATION_CODE,
                        "No active reservation or waiting list found.");
                return;
            }

            // Spec: the code is sent by email AND SMS (each channel the customer has)
            boolean hasEmail = r.email != null && !r.email.isBlank();
            boolean hasPhone = r.phone != null && !r.phone.isBlank();
            boolean isReservation = "RESERVATION".equals(r.type);

            if (hasEmail) EmailService.sendLostCode(r.email, r.code, isReservation);
            if (hasPhone) {
                EmailService.smsStub(r.phone, (isReservation ? "Your reservation code: " : "Your waiting list code: ") + r.code);
            }

            String sentTo = hasEmail && hasPhone ? "by email and SMS"
                    : hasEmail ? "by email"
                    : hasPhone ? "by SMS"
                    : null;
            sendOk(client, OpCode.RESPONSE_RECOVER_CONFIRMATION_CODE, sentTo == null
                    ? "No email or phone is saved for this booking. Please ask a staff member."
                    : "Your confirmation code was sent " + sentTo + ".");

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_RECOVER_CONFIRMATION_CODE,
                        "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Retrieves bill by confirmation code.
     * Response payload is Object[] { ok, alreadyPaid, message, BillDTO }.
     */
    private void handleBillGetByCode(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String code = (payload instanceof String s) ? s.trim() : "";

            BillDAO.BillLookupResult r = BillDAO.getBillByConfirmationCode(code);

            // Object[] { ok, alreadyPaid, message, BillDTO }
            Object[] resp = new Object[] { r.ok, r.alreadyPaid, r.message, r.bill };
            sendOk(client, OpCode.RESPONSE_BILL_GET_BY_CODE, resp);

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_BILL_GET_BY_CODE,
                        new Object[] { false, false, "Server error: " + safeMsg(e), null });
            } catch (Exception ignored) {}
        }
    }
    /**
     * Pays bill by confirmation code.
     * Accepts payload: String code OR Object[] { code, method }.
     * Response payload is Object[] { ok, message, tableId }.
     */
    private void handlePayBill(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();

            // Accept String code OR Object[] { code, method }
            String code;
            if (payload instanceof String s) code = s.trim();
            else if (payload instanceof Object[] arr && arr.length >= 1) code = String.valueOf(arr[0]).trim();
            else {
                sendOk(client, OpCode.RESPONSE_PAY_BILL, new Object[] { false, "Bad payload.", null });
                return;
            }

            BillDAO.PayBillResult r = BillDAO.payBillByConfirmationCode(code);

            // Object[] { ok, message, tableId }
            sendOk(client, OpCode.RESPONSE_PAY_BILL, new Object[] { r.ok, r.message, r.tableId });

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_PAY_BILL, new Object[] { false, "Server error: " + safeMsg(e), null });
            } catch (Exception ignored) {}
        }
    }
    /**
     * Loads the profile of the subscriber identified on this connection.
     * The member code always comes from the session, so a client can only read its own profile.
     */
    private void handleGetProfile(ConnectionToClient client) {
        try {
            String memberCode = sessionMemberCode(client);
            if (memberCode.isBlank()) {
                sendError(client, OpCode.RESPONSE_GET_PROFILE, "Member code missing. Please log in again.");
                return;
            }

            ProfileDTO dto = SubscriberDAO.getProfileByMemberCode(memberCode);
            sendOk(client, OpCode.RESPONSE_GET_PROFILE, dto); // dto may be null

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_GET_PROFILE, null);
            } catch (Exception ignored) {}
        }
    }


    /**
     * Updates subscriber profile (by member number/member_code).
     * Payload must be {@link ProfileDTO}.
     */
    private void handleUpdateProfile(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();

            if (!(payload instanceof ProfileDTO dto)) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Bad payload (expected ProfileDTO).");
                return;
            }

            // The member code is taken from the session: a subscriber can only edit their own profile.
            String memberNumber = sessionMemberCode(client);
            if (memberNumber.isBlank()) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Member code missing. Please log in again.");
                return;
            }

            // basic validation
            String fullName = dto.getFullName() == null ? "" : dto.getFullName().trim();
            String phone    = dto.getPhone() == null ? "" : dto.getPhone().trim();
            String email    = dto.getEmail() == null ? "" : dto.getEmail().trim();

            if (fullName.isBlank() || phone.isBlank() || email.isBlank()) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Full name / phone / email are required.");
                return;
            }
            if (!isValidPhone10Digits(phone)) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Invalid phone (must start with 05 and be 10 digits).");
                return;
            }
            if (!isValidEmailFormat(email)) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Invalid email format.");
                return;
            }

            ProfileDTO update = new ProfileDTO();
            update.setMemberNumber(memberNumber);
            update.setFullName(fullName);
            update.setPhone(phone);
            update.setEmail(email);

            boolean ok;
            try {
                ok = SubscriberDAO.updateProfileByMemberCode(update);
            } catch (SQLIntegrityConstraintViolationException dup) {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE,
                        "This phone or email is already used by another subscriber.");
                return;
            }

            sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE,
                    ok ? "✅ Profile updated." : "❌ Update failed.");

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_UPDATE_PROFILE, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Allows a user to leave the waiting list by confirmation code.
     * Blocks cancel if a visit already exists (checked-in).
     */
    private void handleLeaveWaitingList(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();

            // Expected payload: Object[] { role, username, confirmationCode }
            if (!(payload instanceof Object[] arr) || arr.length < 3) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Bad payload.");
                return;
            }

            // The waiting code itself is the proof of ownership (it is only given to the customer).
            String code = arr[2] == null ? "" : arr[2].toString().trim();

            if (code.isBlank()) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Missing waiting code.");
                return;
            }

            // Load entry
            WaitingListDTO w = WaitingListDAO.getByCode(code);
            if (w == null) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Waiting code not found.");
                return;
            }

            String st = w.getStatus() == null ? "" : w.getStatus().trim();

            if ("CANCELED".equalsIgnoreCase(st)) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Already canceled.");
                return;
            }

            // ✅ Option A: allow cancel in WAITING or ASSIGNED
            if (!"WAITING".equalsIgnoreCase(st) && !"ASSIGNED".equalsIgnoreCase(st)) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Invalid status: " + st);
                return;
            }

            // ✅ Block cancel if already checked-in (Visit exists)
            boolean hasVisit = VisitDAO.existsVisitForWaitingId(w.getId());
            if (hasVisit) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Already checked-in. Can't cancel now.");
                return;
            }

            boolean ok = WaitingListDAO.cancelAndReleaseTablesByCode(code);
            if (!ok) {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Cancel failed (status changed).");
                return;
            }

            w.setStatus("CANCELED");
            sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, w);

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_LEAVE_WAITING_LIST, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Adds a new waiting-list entry for subscriber or guest.
     * Also stores contact in user_activity for later notifications.
     */
    private void handleJoinWaitingList(Envelope req, ConnectionToClient client) {
        // Replies with RESPONSE_WAITING_ADD (RESPONSE_WAITING_LIST is only used for the staff list view)
        final OpCode RESP = OpCode.RESPONSE_WAITING_ADD;
        try {
            Object payload = req.getPayload();

            if (!(payload instanceof Object[] arr) || arr.length < 3 || !(arr[2] instanceof WaitingListDTO dto)) {
                sendOk(client, RESP, "Bad payload.");
                return;
            }

            String role = arr[0] == null ? "" : arr[0].toString();
            String requestedUser = arr[1] == null ? "" : arr[1].toString().trim();

            Timestamp nowCheck = new Timestamp(System.currentTimeMillis());
            if (!OpeningHoursDAO.isOpenForReservation(nowCheck, DINING_MINUTES)) {
                 sendOk(client, RESP, "Restaurant is closed (or closing soon).");
                 return;
            }

            int people = dto.getPeopleCount();
            if (people <= 0) {
                sendOk(client, RESP, "People count must be > 0.");
                return;
            }

            String sizeProblem = partyTooLargeMessage(people);
            if (sizeProblem != null) {
                sendOk(client, RESP, sizeProblem);
                return;
            }

            // Decide who the entry belongs to, based on the server-side session:
            //  - SUBSCRIBER: must be the subscriber identified on this connection
            //  - STAFF:      a logged-in staff member adding a walk-in (subscriber username or "Guest")
            //  - otherwise:  a guest who provides email/phone
            String username = "";
            if ("SUBSCRIBER".equalsIgnoreCase(role)) {
                if (!isSubscriber(client)) {
                    sendOk(client, RESP, "Subscriber not identified. Please scan your QR or log in again.");
                    return;
                }
                username = sessionUser(client);
            } else if ("STAFF".equalsIgnoreCase(role)) {
                if (!isStaff(client)) {
                    sendOk(client, RESP, "Access denied: staff login required.");
                    return;
                }
                if (!requestedUser.isBlank() && !requestedUser.equalsIgnoreCase("Guest")) {
                    username = requestedUser;
                }
            }
            boolean isSubscriberTarget = !username.isBlank();

            String email;
            String phone;

            if (isSubscriberTarget) {
                email = SubscriberDAO.getEmailByUsername(username);
                phone = SubscriberDAO.getPhoneByUsername(username);

                if (phone == null) phone = "";

                if (email == null || email.isBlank()) {
                    sendOk(client, RESP, "Subscriber '" + username + "' not found.");
                    return;
                }

            } else {

                email = dto.getEmail() == null ? "" : dto.getEmail().trim();
                phone = dto.getPhone() == null ? "" : dto.getPhone().trim();

                if (email.isBlank() && phone.isBlank()) {
                    sendOk(client, RESP, "Please provide email or phone (at least one).");
                    return;
                }

                if (!email.isBlank() && !isValidEmailFormat(email)) {
                    sendOk(client, RESP, "Invalid email format.");
                    return;
                }

                if (!phone.isBlank() && !isValidPhone10Digits(phone)) {
                    sendOk(client, RESP, "Invalid phone (must start with 05 and be 10 digits).");
                    return;
                }
            }

            Timestamp now = new Timestamp(System.currentTimeMillis());

            String code = null;
            WaitingListDAO.JoinResult joined = null;

            for (int i = 0; i < 5 && joined == null; i++) {
                code = "W" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();

                try {
                    // waiting_list row + user_activity row (+ immediate seating if a table is free)
                    // are written in ONE transaction.
                    joined = WaitingListDAO.insertWaitingWithActivity(
                            people, now, code,
                            isSubscriberTarget ? username : null,
                            isSubscriberTarget ? null : email,
                            isSubscriberTarget ? null : phone);
                } catch (SQLIntegrityConstraintViolationException dup) {
                    // confirmation_code collision (very unlikely) -> try a new code
                }
            }

            if (joined == null || joined.waitingId <= 0 || code == null) {
                sendOk(client, RESP, "Failed to generate unique waiting code. Please try again.");
                return;
            }

            boolean seatedNow = joined.seatedTableId != null;

            // Give the customer their code by email/SMS as well (staff walk-ins rely on this;
            // the code is also needed later to pay the bill)
            if (seatedNow) {
                if (email != null && !email.isBlank()) EmailService.sendSeatedImmediately(email, code, joined.seatedTableId);
                EmailService.smsStub(phone, "Table " + joined.seatedTableId + " is free - please go to it now. "
                        + "Your code (for the bill): " + code);
            } else {
                if (email != null && !email.isBlank()) EmailService.sendWaitingListJoined(email, code);
                EmailService.smsStub(phone, "You joined the Bistro waiting list. Your code: " + code);
            }

            WaitingListDTO resp = new WaitingListDTO();
            resp.setId(joined.waitingId);
            resp.setPeopleCount(people);
            resp.setStatus(seatedNow ? "ARRIVED" : "WAITING");
            resp.setTableId(joined.seatedTableId);
            resp.setConfirmationCode(code);
            resp.setEmail(email);
            resp.setPhone(phone);

            sendOk(client, RESP, resp);

        } catch (Exception e) {
            try {
                sendOk(client, RESP, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    
    /**
     * Cancels reservation by reservationId with identity check.
     * Payload expected: Object[] { role, username, guestEmail, guestPhone, reservationId }.
     */
    @SuppressWarnings("unchecked")
    private void handleCancelReservation(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();

            // Expected payload: Object[] { role, username, guestEmail, guestPhone, reservationId }
            if (!(payload instanceof Object[] arr) || arr.length < 5) {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION,
                        "Bad payload. Expected {role, username, email, phone, reservationId}.");
                return;
            }

            String role = arr[0] == null ? null : String.valueOf(arr[0]);
            // Never trust the username from the payload: a subscriber can only cancel their own reservations.
            String username = isSubscriber(client) ? sessionUser(client) : null;
            String guestEmail = arr[2] == null ? null : String.valueOf(arr[2]);
            String guestPhone = arr[3] == null ? null : String.valueOf(arr[3]);

            int reservationId;
            try {
                reservationId = (arr[4] instanceof Number n) ? n.intValue() : Integer.parseInt(String.valueOf(arr[4]));
            } catch (Exception ex) {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Bad reservation id.");
                return;
            }

            boolean isSubscriber = role != null && role.equalsIgnoreCase("SUBSCRIBER");
            boolean isCustomer = role != null && role.equalsIgnoreCase("CUSTOMER");

            if (!isSubscriber && !isCustomer) {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Unknown role.");
                return;
            }

            if (isSubscriber) {
                if (username == null || username.isBlank()) {
                    sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Subscriber not logged in. Please log in again.");
                    return;
                }
            } else {
                // Spec: a guest identifies with phone and/or email
                boolean noEmail = guestEmail == null || guestEmail.isBlank();
                boolean noPhone = guestPhone == null || guestPhone.isBlank();
                if (noEmail && noPhone) {
                    sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Please enter your phone and/or email.");
                    return;
                }
            }

            // Call DAO
            // This method should:
            // 1) Verify reservation belongs to the requester
            // 2) Verify status is cancel-able (e.g., CONFIRMED)
            // 3) Update status to CANCELLED
            boolean ok = ReservationDAO.cancelReservation(
                    reservationId,
                    role,
                    username,
                    guestEmail,
                    guestPhone
            );

            if (ok) {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Reservation cancelled.");
            } else {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Cancel failed (not found / not yours / not active).");
            }


        } catch (Exception e) {
            log("Cancel reservation failed: " + e.getMessage());
            try {
                sendOk(client, OpCode.RESPONSE_CANCEL_RESERVATION, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {
                log("Also failed to send response to client: " + ignored.getMessage());
            }
        }
    }
    /**
     * Returns current diners (active visits) for dashboard.
     */
    private void handleCurrentDiners(ConnectionToClient client) {
        try {
            List<common.dto.CurrentDinersDTO> list = VisitDAO.getActiveDiners();
            sendOk(client, OpCode.RESPONSE_CURRENT_DINERS, list);
        } catch (Exception e) {
            log("Error fetching current diners: " + e.getMessage());
            // Always reply, otherwise the staff screen waits forever
            try { sendError(client, OpCode.RESPONSE_CURRENT_DINERS, "Fetch current diners failed: " + safeMsg(e)); }
            catch (Exception ignored) {}
        }
    }
    /**
     * Sends list of restaurant tables.
     */
    private void handleGetTables(ConnectionToClient client) {
        try {
            List<common.dto.RestaurantTableDTO> list = DataBase.dao.RestaurantTableDAO.getAllTables();
            sendOk(client, OpCode.RESPONSE_TABLES_GET, list);
        } catch (Exception e) {
            try { sendError(client, OpCode.ERROR, "Fetch tables failed: " + e.getMessage()); } catch (Exception ignored) {}
        }
    }
    /**
     * Adds a table (or updates it depending on DAO behavior).
     */
    private void handleAddTable(Envelope req, ConnectionToClient client) {
        try {
            if (!(req.getPayload() instanceof RestaurantTableDTO dto)) {
                sendError(client, OpCode.RESPONSE_TABLE_ADD, "Bad payload (expected RestaurantTableDTO).");
                return;
            }

            int seats = dto.getSeats();
            if (seats <= 0 || seats > 50) {
                sendError(client, OpCode.RESPONSE_TABLE_ADD, "Seats must be between 1 and 50.");
                return;
            }

            String newId = RestaurantTableDAO.insertTableAutoId(seats);
            sendOk(client, OpCode.RESPONSE_TABLE_ADD, "Table " + newId + " added (" + seats + " seats).");
        } catch (Exception e) {
            try {
                sendError(client, OpCode.RESPONSE_TABLE_ADD, "Add table failed: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Removes a table by tableId, only if it is safe (FREE, not in use, not needed by reservations).
     */
    private void handleRemoveTable(Envelope req, ConnectionToClient client) {
        try {
            String tableId = (req.getPayload() instanceof String s) ? s.trim() : "";

            String problem = RestaurantTableDAO.isSafeToDelete(tableId);
            if (problem != null) {
                sendError(client, OpCode.RESPONSE_TABLE_REMOVE, problem);
                return;
            }

            boolean ok;
            try {
                ok = RestaurantTableDAO.deleteTable(tableId);
            } catch (SQLIntegrityConstraintViolationException fk) {
                // visit.table_id references this table: keep it so past visits / reports stay valid
                sendError(client, OpCode.RESPONSE_TABLE_REMOVE,
                        "Table " + tableId + " has visit history and cannot be deleted (it is kept for reports).");
                return;
            }

            if (ok) sendOk(client, OpCode.RESPONSE_TABLE_REMOVE, "Table " + tableId + " removed.");
            else sendError(client, OpCode.RESPONSE_TABLE_REMOVE, "Table not found.");
        } catch (Exception e) {
            try { sendError(client, OpCode.RESPONSE_TABLE_REMOVE, "Remove table failed: " + safeMsg(e)); }
            catch (Exception ignored) {}
        }
    }
    /**
     * Updates seats of an existing table, only if existing reservations still fit afterwards.
     */
    private void handleUpdateTable(Envelope req, ConnectionToClient client) {
        try {
            if (!(req.getPayload() instanceof RestaurantTableDTO dto)) {
                sendError(client, OpCode.RESPONSE_TABLE_UPDATE, "Bad payload (expected RestaurantTableDTO).");
                return;
            }
            if (dto.getSeats() <= 0 || dto.getSeats() > 50) {
                sendError(client, OpCode.RESPONSE_TABLE_UPDATE, "Seats must be between 1 and 50.");
                return;
            }

            String problem = RestaurantTableDAO.isSafeToChangeSeats(dto.getTableId(), dto.getSeats());
            if (problem != null) {
                sendError(client, OpCode.RESPONSE_TABLE_UPDATE, problem);
                return;
            }

            boolean ok = RestaurantTableDAO.updateTableSeats(dto.getTableId(), dto.getSeats());
            if (ok) sendOk(client, OpCode.RESPONSE_TABLE_UPDATE, "Table updated.");
            else sendError(client, OpCode.RESPONSE_TABLE_UPDATE, "Table not found.");
        } catch (Exception e) {
            try { sendError(client, OpCode.RESPONSE_TABLE_UPDATE, "Update table failed: " + safeMsg(e)); }
            catch (Exception ignored) {}
        }
    }
    /**
     * Returns all opening hours rows.
     */
    private void handleGetOpeningHours(ConnectionToClient client) {
        try {
            List<common.dto.OpeningHoursDTO> list = DataBase.dao.OpeningHoursDAO.getAllOpeningHours();
            sendOk(client, OpCode.RESPONSE_OPENING_HOURS_GET, list);
        } catch (Exception e) {
            try { sendError(client, OpCode.ERROR, "Fetch hours failed: " + e.getMessage()); } catch (Exception ignored) {}
        }
    }
    /**
     * Handles a request to update the opening hours for a specific day or time slot.
     * <p>
     * <b>Workflow:</b>
     * <ol>
     * <li>Updates the {@code opening_hours} table using the ID provided in the DTO.</li>
     * <li>If successful, triggers a conflict check using {@link ReservationDAO#cancelConflictsAfterHoursUpdate}.</li>
     * <li>If any reservations are auto-canceled, iterates through the list and sends 
     * notifications (Email/SMS) via {@link #notifyCanceledCustomers}.</li>
     * </ol>
     * </p>
     *
     * @param req The request envelope containing the {@link OpeningHoursDTO}.
     * @param client The client connection triggered the request.
     */
    private void handleUpdateOpeningHours(Envelope req, ConnectionToClient client) {
        final OpCode RESP = OpCode.RESPONSE_OPENING_HOURS_UPDATE;
        try {
            if (!(req.getPayload() instanceof OpeningHoursDTO dto)) {
                sendError(client, RESP, "Bad payload (expected OpeningHoursDTO).");
                return;
            }

            String[] times = normalizeHours(dto.getOpenTime(), dto.getCloseTime());
            if (times == null) {
                sendError(client, RESP, HOURS_FORMAT_HELP);
                return;
            }

            OpeningHoursDTO existing = OpeningHoursDAO.getById(dto.getHoursId());
            if (existing == null) {
                sendError(client, RESP, "Opening-hours row not found.");
                return;
            }

            boolean ok = OpeningHoursDAO.updateOpeningHour(dto.getHoursId(), times[0], times[1]);
            if (!ok) {
                sendError(client, RESP, "Update failed.");
                return;
            }

            // Use the DB row (not the client's DTO) to know which day/date is affected
            String specialDate = existing.isSpecial() ? existing.getSpecialDate() : null;
            String dayOfWeek = existing.isSpecial() ? null : existing.getDayOfWeek();

            List<String[]> affected = ReservationDAO.cancelConflictsAfterHoursUpdate(
                    dayOfWeek, specialDate, times[0], times[1]);

            int count = (affected == null) ? 0 : affected.size();
            if (count > 0) {
                String dateLabel = (specialDate != null) ? specialDate : "Every " + dayOfWeek;
                notifyCanceledCustomers(affected, dateLabel);
            }

            sendOk(client, RESP, "Hours updated" + (count > 0
                    ? ". " + count + " conflicting reservation(s) were canceled and notified." : "."));
        } catch (Exception e) {
            try { sendError(client, RESP, "Update error: " + safeMsg(e)); } catch (Exception ignored) {}
        }
    }

    private static final String HOURS_FORMAT_HELP =
            "Times must be HH:mm with open before close (use the same time for both, e.g. 00:00-00:00, to mark the day closed).";

    /**
     * Validates and normalizes opening hours.
     *
     * @return {open, close} as "HH:mm", or null if invalid. open == close means "closed".
     */
    private static String[] normalizeHours(String open, String close) {
        LocalTime o = parseHHmm(open);
        LocalTime c = parseHHmm(close);
        if (o == null || c == null) return null;
        if (c.isBefore(o)) return null; // closing after midnight is not supported
        return new String[] { String.format("%02d:%02d", o.getHour(), o.getMinute()),
                              String.format("%02d:%02d", c.getHour(), c.getMinute()) };
    }

    private static LocalTime parseHHmm(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (!t.matches("^([01]?\\d|2[0-3]):[0-5]\\d(:[0-5]\\d)?$")) return null;
        if (t.length() == 4 || t.indexOf(':') == 1) t = "0" + t; // 9:00 -> 09:00
        return LocalTime.parse(t.length() > 5 ? t.substring(0, 5) : t);
    }
    /**
     * Handles a request to add a special opening hour (exception date).
     * <p>
     * <b>Workflow:</b>
     * <ol>
     * <li>Inserts a new record into {@code opening_hours} with {@code is_special='YES'}.</li>
     * <li>Triggers a conflict check for that specific date using {@link ReservationDAO#cancelConflictsAfterHoursUpdate}.</li>
     * <li>If the new special hours (e.g., closing early) conflict with existing bookings, 
     * those bookings are canceled and customers are notified.</li>
     * </ol>
     * </p>
     *
     * @param req The request envelope containing the {@link OpeningHoursDTO}.
     * @param client The client connection triggered the request.
     */
    private void handleAddSpecialHour(Envelope req, ConnectionToClient client) {
        final OpCode RESP = OpCode.RESPONSE_OPENING_HOURS_ADD_SPECIAL;
        try {
            if (!(req.getPayload() instanceof OpeningHoursDTO dto)) {
                sendError(client, RESP, "Bad payload (expected OpeningHoursDTO).");
                return;
            }

            LocalDate date;
            try {
                date = LocalDate.parse(dto.getSpecialDate() == null ? "" : dto.getSpecialDate().trim());
            } catch (Exception e) {
                sendError(client, RESP, "Special date must be YYYY-MM-DD.");
                return;
            }
            if (date.isBefore(LocalDate.now())) {
                sendError(client, RESP, "Special date cannot be in the past.");
                return;
            }

            String[] times = normalizeHours(dto.getOpenTime(), dto.getCloseTime());
            if (times == null) {
                sendError(client, RESP, HOURS_FORMAT_HELP);
                return;
            }

            // Adds the date, or updates it if that date already has special hours
            OpeningHoursDAO.insertSpecialHour(date.toString(), null, times[0], times[1]);

            List<String[]> affected = ReservationDAO.cancelConflictsAfterHoursUpdate(
                    null, date.toString(), times[0], times[1]);

            String msg = "Special hours saved for " + date + (times[0].equals(times[1]) ? " (closed)." : ".");
            if (affected != null && !affected.isEmpty()) {
                // Customers whose reservation was canceled must be told (this was missing)
                notifyCanceledCustomers(affected, date.toString());
                msg += " " + affected.size() + " conflicting reservation(s) were canceled and notified.";
            }

            sendOk(client, RESP, msg);

        } catch (Exception e) {
            try {
                sendError(client, RESP, "Add special date failed: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Removes a special-date row by ID. Reservations on that date then follow the regular weekly
     * hours again, so they are re-checked against those hours (and canceled + notified if they no
     * longer fit). Regular weekly rows cannot be removed (edit them instead).
     */
    private void handleRemoveSpecialHour(Envelope req, ConnectionToClient client) {
        final OpCode RESP = OpCode.RESPONSE_OPENING_HOURS_REMOVE;
        try {
            if (!(req.getPayload() instanceof Integer id)) {
                sendError(client, RESP, "Bad payload (expected hours id).");
                return;
            }

            OpeningHoursDTO row = OpeningHoursDAO.getById(id);
            if (row == null) {
                sendError(client, RESP, "Special date not found.");
                return;
            }
            if (!row.isSpecial()) {
                sendError(client, RESP, "Regular weekly hours cannot be removed. Edit them instead.");
                return;
            }

            if (!OpeningHoursDAO.deleteOpeningHour(id)) {
                sendError(client, RESP, "Special date not found.");
                return;
            }

            String msg = "Special date " + row.getSpecialDate() + " removed.";
            LocalDate date = LocalDate.parse(row.getSpecialDate());
            String dayName = date.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH);
            String[] weekly = OpeningHoursDAO.getWeeklyHours(dayName);
            List<String[]> affected = (weekly == null)
                    // no weekly hours for that day = closed -> every reservation that day conflicts
                    ? ReservationDAO.cancelConflictsAfterHoursUpdate(null, date.toString(), "00:00", "00:00")
                    : ReservationDAO.cancelConflictsAfterHoursUpdate(null, date.toString(), weekly[0], weekly[1]);
            if (affected != null && !affected.isEmpty()) {
                notifyCanceledCustomers(affected, date.toString());
                msg += " " + affected.size() + " reservation(s) did not fit the regular hours and were canceled and notified.";
            }
            sendOk(client, RESP, msg);
        } catch (Exception e) {
            try { sendError(client, RESP, "Remove failed: " + safeMsg(e)); } catch (Exception ignored) {}
        }
    }
    /**
     * Returns opening-hours string for today (used in UI for quick display).
     */
    private void handleGetTodayHours(ConnectionToClient client) {
        try {
            String hours = DataBase.dao.OpeningHoursDAO.getHoursForDate(java.time.LocalDate.now());
            sendOk(client, OpCode.RESPONSE_TODAY_HOURS, hours);
        } catch (Exception e) {
            try { sendError(client, OpCode.ERROR, "Failed to get today's hours"); } catch (Exception ignored) {}
        }
    }
    
    
    /**
     * Creates performance report data for given month/year.
     */
    private void handleReportPerformance(Envelope req, ConnectionToClient client) {
        try {
            common.dto.ReportRequestDTO dto = (common.dto.ReportRequestDTO) req.getPayload();
            List<common.dto.ReportDTO> data = DataBase.dao.ReportDAO.getPerformanceReport(dto.getMonth(), dto.getYear());
            sendOk(client, OpCode.RESPONSE_REPORT_PERFORMANCE, data);
        } catch (Exception e) {
            e.printStackTrace();
            try { sendError(client, OpCode.ERROR, "Perf Report Failed"); } catch (Exception ignored) {}
        }
    }
    /**
     * Creates subscriber activity report data for given month/year.
     */
    private void handleReportActivity(Envelope req, ConnectionToClient client) {
        try {
            common.dto.ReportRequestDTO dto = (common.dto.ReportRequestDTO) req.getPayload();
            List<common.dto.ReportDTO> data = DataBase.dao.ReportDAO.getSubscriberActivityReport(dto.getMonth(), dto.getYear());
            sendOk(client, OpCode.RESPONSE_REPORT_ACTIVITY, data);
        } catch (Exception e) {
            e.printStackTrace();
            try { sendError(client, OpCode.ERROR, "Activity Report Failed"); } catch (Exception ignored) {}
        }
    }


    // -----------------------------------------------------------
    // 1. REGISTRATION
    // -----------------------------------------------------------
    /**
     * Registers a new subscriber (customer registration screen).
     * Payload must be {@link RegistrationDTO}.
     */
    private void handleRegisterCustomer(Envelope req, ConnectionToClient client) throws IOException {
        final OpCode RESP = OpCode.RESPONSE_REGISTER_CUSTOMER;
        try {
            if (!(req.getPayload() instanceof RegistrationDTO dto)) {
                sendError(client, RESP, "Bad payload (expected RegistrationDTO).");
                return;
            }

            String username = dto.getUsername() == null ? "" : dto.getUsername().trim();
            String password = dto.getPassword() == null ? "" : dto.getPassword();
            String fullName = dto.getFullName() == null ? "" : dto.getFullName().trim();
            String phone    = dto.getPhone() == null ? "" : dto.getPhone().trim();
            String email    = dto.getEmail() == null ? "" : dto.getEmail().trim();

            // ---- Validation (never insert blank / invalid subscribers) ----
            String error = null;
            if (!username.matches("^[A-Za-z0-9_.-]{3,50}$")) {
                error = "Username must be 3-50 characters (letters, digits, . _ -).";
            } else if (password.length() < 4 || password.length() > 64) {
                error = "Password must be 4-64 characters.";
            } else if (fullName.isEmpty() || fullName.length() > 100) {
                error = "Full name is required (max 100 characters).";
            } else if (!isValidPhone10Digits(phone)) {
                error = "Invalid phone (must start with 05 and be 10 digits).";
            } else if (!isValidEmailFormat(email) || email.length() > 100) {
                error = "Invalid email address.";
            }

            Date sqlBirthDate = null;
            if (error == null) {
                try {
                    LocalDate bd = LocalDate.parse(dto.getBirthDate() == null ? "" : dto.getBirthDate().trim());
                    if (bd.isAfter(LocalDate.now()) || bd.isBefore(LocalDate.now().minusYears(120))) {
                        error = "Birth date is not valid.";
                    } else {
                        sqlBirthDate = Date.valueOf(bd);
                    }
                } catch (Exception e) {
                    error = "Birth date is required (YYYY-MM-DD).";
                }
            }

            if (error != null) {
                sendError(client, RESP, "Registration failed: " + error);
                return;
            }

            try {
                SubscriberDAO.insertSubscriber(username, password, fullName, phone, email, sqlBirthDate);
            } catch (SQLIntegrityConstraintViolationException dup) {
                // Do not echo the SQL message (it would reveal which column / value already exists)
                sendError(client, RESP, "Registration failed: username, phone or email is already registered.");
                return;
            }

            log("Registered new subscriber: " + username);
            sendOk(client, RESP, "Customer registered successfully.");

        } catch (Exception e) {
            log("Error registering customer: " + e.getMessage());
            sendError(client, RESP, "Registration failed: " + safeMsg(e));
        }
    }

    // -----------------------------------------------------------
    // 2. MAKE RESERVATION
    // -----------------------------------------------------------
    /**
     * Validates email format with a simple regex.
     *
     * @param email raw email
     * @return true if matches pattern
     */
    private static boolean isValidEmailFormat(String email) {
        if (email == null) return false;
        String e = email.trim();
        return e.matches("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    }
    /**
     * Validates Israeli phone format: must start with 05 and be exactly 10 digits.
     *
     * @param phone raw phone string
     * @return true if matches pattern
     */
    private static boolean isValidPhone10Digits(String phone) {
        if (phone == null) return false;
        String p = phone.trim();
        return p.matches("^05\\d{8}$");
    }
    /**
     * Creates a reservation after validating constraints:
     * <ul>
     *   <li>guest must provide valid email + phone</li>
     *   <li>reservation up to 1 month ahead</li>
     *   <li>reservation at least 1 hour from now</li>
     *   <li>capacity check + alternative suggestions</li>
     * </ul>
     * Sends confirmation email/SMS stub on success.
     */
    private void handleMakeReservation(Envelope req, ConnectionToClient client) throws Exception {
        final OpCode RESP = OpCode.RESPONSE_MAKE_RESERVATION;

        if (!(req.getPayload() instanceof MakeReservationRequestDTO raw)) {
            sendError(client, RESP, "Bad payload: expected MakeReservationRequestDTO");
            return;
        }

        String timeError = validateReservationRequest(raw);
        if (timeError != null) {
            sendOk(client, RESP, new MakeReservationResponseDTO(false, -1, null, timeError));
            return;
        }

        // ---- Decide who owns the reservation from the server-side session (never from the payload) ----
        String subscriberUsername = null;
        String guestEmail = null;
        String guestPhone = null;
        String requested = raw.getSubscriberUsername() == null ? "" : raw.getSubscriberUsername().trim();

        if (isSubscriber(client)) {
            subscriberUsername = sessionUser(client);
        } else if (isStaff(client) && !requested.isEmpty()) {
            // staff booking on behalf of a subscriber
            if (SubscriberDAO.getEmailByUsername(requested) == null) {
                sendOk(client, RESP, new MakeReservationResponseDTO(false, -1, null,
                        "Subscriber '" + requested + "' not found."));
                return;
            }
            subscriberUsername = requested;
        } else {
            String ge = raw.getGuestEmail() == null ? "" : raw.getGuestEmail().trim();
            String gp = raw.getGuestPhone() == null ? "" : raw.getGuestPhone().trim();

            if (ge.isEmpty() && gp.isEmpty()) {
                sendOk(client, RESP, new MakeReservationResponseDTO(false, -1, null, "Guest must provide Email OR Phone."));
                return;
            }
            if (!ge.isEmpty() && !isValidEmailFormat(ge)) {
                sendOk(client, RESP, new MakeReservationResponseDTO(false, -1, null, "Invalid guest email."));
                return;
            }
            if (!gp.isEmpty() && !isValidPhone10Digits(gp)) {
                sendOk(client, RESP, new MakeReservationResponseDTO(false, -1, null,
                        "Invalid guest phone (must start with 05 and be 10 digits)."));
                return;
            }
            guestEmail = ge.isEmpty() ? null : ge;
            guestPhone = gp.isEmpty() ? null : gp;
        }

        MakeReservationRequestDTO dto = new MakeReservationRequestDTO(
                subscriberUsername, guestPhone, guestEmail, raw.getNumOfCustomers(), raw.getReservationTime());

        // Capacity is checked again INSIDE the create (atomically), so two bookings at once can't overbook.
        ReservationDAO.CreateReservationResult r;
        try {
            r = reservationDAO.createReservationWithActivity(dto);
        } catch (ReservationDAO.NoCapacityException full) {
            List<Timestamp> suggestions = findAlternativeTimes(dto.getReservationTime(), dto.getNumOfCustomers());
            sendOk(client, RESP, new MakeReservationResponseDTO(false, "No available seats at requested time.", suggestions));
            return;
        }

        // Notify the customer (sending is asynchronous, it never blocks this request)
        String toEmail = guestEmail;
        String phone = guestPhone;
        if (subscriberUsername != null) {
            toEmail = SubscriberDAO.getEmailByUsername(subscriberUsername);
            phone = SubscriberDAO.getPhoneByUsername(subscriberUsername);
        }
        if (toEmail != null && !toEmail.isBlank()) {
            EmailService.sendReservationConfirmation(toEmail, r.confirmationCode);
        }
        EmailService.smsStub(phone, "Reservation has been made | code: " + r.confirmationCode);

        sendOk(client, RESP, new MakeReservationResponseDTO(true, r.reservationId, r.confirmationCode,
                "Reservation created successfully!"));
    }

    /**
     * Rules shared by "check availability" and "make reservation", so both always agree:
     * party size, 1 hour minimum notice, 1 month maximum, slot alignment and opening hours.
     *
     * @return an error message, or {@code null} if the requested time is acceptable
     */
    private static String validateReservationRequest(MakeReservationRequestDTO dto) throws Exception {
        if (dto.getNumOfCustomers() <= 0 || dto.getReservationTime() == null) return "Invalid input.";

        java.time.LocalDateTime when = dto.getReservationTime().toLocalDateTime();
        java.time.LocalDateTime now = java.time.LocalDateTime.now();

        if (when.isAfter(now.plusMonths(1))) return "Reservation can be made up to 1 month ahead.";
        if (when.isBefore(now.plusHours(1))) return "Reservation must be at least 1 hour from now.";
        if (when.getMinute() % SLOT_MINUTES != 0 || when.getSecond() != 0) {
            return "Reservations start on the hour or half hour.";
        }
        if (!OpeningHoursDAO.isOpenForReservation(dto.getReservationTime(), DINING_MINUTES)) {
            return "The restaurant is closed at the selected time.";
        }
        return partyTooLargeMessage(dto.getNumOfCustomers());
    }

    /**
     * Each party sits at ONE whole table (spec), so a party bigger than the largest table can never be seated.
     *
     * @return an error message, or null if some table is big enough
     */
    private static String partyTooLargeMessage(int people) throws Exception {
        int maxTable = RestaurantTableDAO.getMaxTableSeats();
        if (maxTable <= 0) return "Restaurant tables are not configured. Please contact staff.";
        if (people > maxTable) {
            return "Our largest table seats " + maxTable + " people. Please contact the restaurant for larger groups.";
        }
        return null;
    }

    // -----------------------------------------------------------
    // 3. OTHER HANDLERS
    // -----------------------------------------------------------
    /**
     * Checks capacity availability without creating a reservation.
     * Returns alternative suggestions if cannot fit.
     */
    private void handleCheckAvailability(Envelope req, ConnectionToClient client) throws Exception {
        Object payloadObj = req.getPayload();
        if (!(payloadObj instanceof MakeReservationRequestDTO dto)) {
            sendError(client, OpCode.RESPONSE_CHECK_AVAILABILITY, "Bad payload.");
            return;
        }

        // Same rules as making the reservation, so "Available" really means it can be booked
        String timeError = validateReservationRequest(dto);
        if (timeError != null) {
            sendOk(client, OpCode.RESPONSE_CHECK_AVAILABILITY, new MakeReservationResponseDTO(false, -1, null, timeError));
            return;
        }

        boolean canFit = ReservationDAO.canFitAtTime(dto.getReservationTime(), dto.getNumOfCustomers());

        if (!canFit) {
            List<Timestamp> suggestions =
                    findAlternativeTimes(dto.getReservationTime(), dto.getNumOfCustomers());
            sendOk(client, OpCode.RESPONSE_CHECK_AVAILABILITY,
                    new MakeReservationResponseDTO(false, "No available seats at requested time.", suggestions));
            return;
        }

        sendOk(client, OpCode.RESPONSE_CHECK_AVAILABILITY,
                new MakeReservationResponseDTO(true, -1, null, "Available"));

    }
    /**
     * Validates a confirmation code at the terminal:
     * <ul>
     *   <li>first checks reservation code</li>
     *   <li>if not reservation → checks waiting-list code</li>
     *   <li>maps waiting-list status into valid/check-in allowed flags</li>
     * </ul>
     */
    private void handleTerminalValidateCode(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String code = (payload instanceof String s) ? s : null;

            if (code == null || code.isBlank()) {
                sendOk(client, OpCode.RESPONSE_TERMINAL_VALIDATE_CODE,
                        new TerminalValidateResponseDTO(false, "Invalid confirmation code."));
                return;
            }

            code = code.trim();

            // 1) RESERVATION code: if the reservation exists, its answer is final
            //    (also when it's canceled/expired: show that message instead of "Code not found")
            TerminalValidateResponseDTO info = ReservationDAO.getTerminalInfoByCode(code);
            if (info != null && info.getReservationId() > 0) {
                sendOk(client, OpCode.RESPONSE_TERMINAL_VALIDATE_CODE, info);
                return;
            }

         // 2) Try WAITING LIST code (NEW)
            WaitingListDTO w = WaitingListDAO.getByCode(code);
            if (w == null) {
                sendOk(client, OpCode.RESPONSE_TERMINAL_VALIDATE_CODE,
                        new TerminalValidateResponseDTO(false, "Code not found."));
                return;
            }

            TerminalValidateResponseDTO dto = new TerminalValidateResponseDTO();

            // Not a reservation
            dto.setReservationId(0);
            dto.setNumOfCustomers(w.getPeopleCount());
            dto.setStatus(w.getStatus());

            String st = (w.getStatus() == null) ? "" : w.getStatus().trim();

            if ("ASSIGNED".equalsIgnoreCase(st)) {
                dto.setValid(true);
                dto.setCheckInAllowed(true);
                dto.setMessage("Table is assigned. You may check-in now.");
            } else if ("WAITING".equalsIgnoreCase(st)) {
                dto.setValid(true);
                dto.setCheckInAllowed(false);
                dto.setMessage("You are in the waiting list. Please wait…");
            } else if ("CANCELED".equalsIgnoreCase(st)) {
                // ✅ IMPORTANT: expired/canceled should NOT be valid
                dto.setValid(false);
                dto.setCheckInAllowed(false);
                dto.setMessage("This waiting code has expired or was canceled.");
            } else {
                // Unknown status -> treat as invalid
                dto.setValid(false);
                dto.setCheckInAllowed(false);
                dto.setMessage("Invalid waiting list status.");
            }

            sendOk(client, OpCode.RESPONSE_TERMINAL_VALIDATE_CODE, dto);
            return;

        } catch (Exception e) {
            try {
                sendError(client, OpCode.RESPONSE_TERMINAL_VALIDATE_CODE, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Performs terminal check-in:
     * <ul>
     *   <li>reservation check-in: marks ARRIVED or PENDING (if no table yet)</li>
     *   <li>waiting-list check-in: delegates to {@link WaitingListDAO#checkInWaitingListByCode(String)}</li>
     * </ul>
     */
    private void handleTerminalCheckIn(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            String code = (payload instanceof String s) ? s : null;

            if (code == null || code.isBlank()) {
                sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN,
                        new TerminalValidateResponseDTO(false, "Invalid confirmation code."));
                return;
            }

            code = code.trim();
            
            // 1) RESERVATION check-in
            TerminalValidateResponseDTO info = ReservationDAO.getTerminalInfoByCode(code);
            if (info != null && info.getReservationId() > 0) {
                if (!info.isValid()) {
                    // canceled / finished reservation: report it, nothing to check in
                    sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN, info);
                    return;
                }

                String tableId;
                try {
                    tableId = ReservationDAO.markArrivedByCodeReturnTableId(code);
                } catch (SQLException dbErr) {
                    throw dbErr; // real server problem -> generic handler below
                } catch (Exception rule) {
                    // Business rule (too early, too late/no-show, moved to PENDING...): show it as-is,
                    // not as a "Server error".
                    TerminalValidateResponseDTO ruleDto = ReservationDAO.getTerminalInfoByCode(code);
                    ruleDto.setValid(false);
                    ruleDto.setCheckInAllowed(false);
                    ruleDto.setTableId("-");
                    ruleDto.setMessage(rule.getMessage());
                    sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN, ruleDto);
                    return;
                }

                TerminalValidateResponseDTO dto = ReservationDAO.getTerminalInfoByCode(code);
                dto.setValid(true);
                dto.setCheckInAllowed(false);
                dto.setStatus("ARRIVED");
                dto.setTableId(tableId);
                dto.setMessage("Checked-in successfully. Table: " + tableId);

                sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN, dto);
                return;
            }

            // 2) WAITING LIST check-in (ALL DB WORK INSIDE DAO)
            TerminalValidateResponseDTO dto = WaitingListDAO.checkInWaitingListByCode(code);

            sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN, dto);

        } catch (Exception e) {
            TerminalValidateResponseDTO err = new TerminalValidateResponseDTO();
            err.setValid(false);
            err.setMessage("Server error: " + safeMsg(e));
            try {
                sendOk(client, OpCode.RESPONSE_TERMINAL_CHECK_IN, err);
            } catch (Exception ignored) {}
        }
    }


    /**
     * Handles subscriber login (returns member_code for profile flow).
     */
    private void handleLoginSubscriber(Envelope env, ConnectionToClient client) {
        try {
            if (!(env.getPayload() instanceof LoginRequestDTO req)) {
                sendOk(client, OpCode.RESPONSE_LOGIN_SUBSCRIBER,
                        new LoginResponseDTO(false, "Bad login request.", null, null, null));
                return;
            }

            String user = req.getUsername() == null ? "" : req.getUsername().trim();
            boolean ok = SubscriberDAO.checkLogin(user, req.getPassword());

            LoginResponseDTO res;

            if (ok) {
                // member_code ONLY flow -> must fetch it and send it back
                String memberCode = SubscriberDAO.getMemberCodeByUsername(user);

                if (memberCode == null || memberCode.isBlank()) {
                    res = new LoginResponseDTO(false, "Login failed: member code not found for this user.",
                            null, null, null);
                } else {
                    // Remember who is logged in on this connection (used for all later authorization)
                    setSession(client, "SUBSCRIBER", user, memberCode.trim());
                    res = new LoginResponseDTO(true, "Login success", user, "SUBSCRIBER", memberCode.trim());
                }
            } else {
                // A failed login does NOT change the current session (e.g. staff "act as subscriber")
                res = new LoginResponseDTO(false, "Invalid username or password", null, null, null);
            }

            sendOk(client, OpCode.RESPONSE_LOGIN_SUBSCRIBER, res);

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_LOGIN_SUBSCRIBER,
                        new LoginResponseDTO(false, "Server error: " + safeMsg(e), null, null, null));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Handles staff login and returns staff role (AGENT/MANAGER).
     */
    private void handleLoginStaff(Envelope env, ConnectionToClient client) {
        try {
            if (!(env.getPayload() instanceof LoginRequestDTO req)) {
                sendOk(client, OpCode.RESPONSE_LOGIN_STAFF,
                        new LoginResponseDTO(false, "Bad login request.", null, null, null));
                return;
            }

            String user = req.getUsername() == null ? "" : req.getUsername().trim();
            String role = StaffDAO.checkLoginAndGetRole(user, req.getPassword());

            LoginResponseDTO res;
            if (role != null) {
                setSession(client, role.trim().toUpperCase(), user, null);
                res = new LoginResponseDTO(true, "Login success", user, role.trim(), null);
            } else {
                res = new LoginResponseDTO(false, "Invalid username or password", null, null, null);
            }

            sendOk(client, OpCode.RESPONSE_LOGIN_STAFF, res);
        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_LOGIN_STAFF,
                        new LoginResponseDTO(false, "Server error: " + safeMsg(e), null, null, null));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Returns reservation list for subscriber or guest based on role.
     * Payload expected: Object[] { role, username, email, phone }.
     */
    private void handleReservationsList(Envelope req, ConnectionToClient client) throws Exception {
        if (!(req.getPayload() instanceof Object[] arr) || arr.length < 4) {
            sendError(client, OpCode.RESPONSE_RESERVATIONS_LIST, "Bad payload.");
            return;
        }
        String role = arr[0] == null ? "" : arr[0].toString();
        String email = arr[2] == null ? "" : arr[2].toString().trim();
        String phone = arr[3] == null ? "" : arr[3].toString().trim();

        List<Reservation> rows;
        if ("SUBSCRIBER".equalsIgnoreCase(role)) {
            if (!isSubscriber(client)) {
                sendError(client, OpCode.RESPONSE_RESERVATIONS_LIST, "Subscriber not logged in. Please log in again.");
                return;
            }
            rows = reservationDAO.getReservationsBySubscriber(sessionUser(client));
        } else {
            // Spec: a guest identifies with phone and/or email (same rule as cancel)
            if (email.isBlank() && phone.isBlank()) {
                sendError(client, OpCode.RESPONSE_RESERVATIONS_LIST, "Please enter your phone and/or email.");
                return;
            }
            rows = reservationDAO.getReservationsByGuest(email, phone);
        }

        List<ReservationDTO> dtoList = new ArrayList<>();
        for (Reservation r : rows) {
            dtoList.add(toReservationDTO(r));
        }
        sendOk(client, OpCode.RESPONSE_RESERVATIONS_LIST, dtoList);
    }

    /**
     * Sends list of subscribers for staff view.
     */
    private void handleSubscribersList(ConnectionToClient client) {
        try {
            List<SubscriberDTO> list = SubscriberDAO.getAllSubscribers();
            sendOk(client, OpCode.RESPONSE_SUBSCRIBERS_LIST, list);
        } catch (Exception e) {
            log("Error fetching subscribers: " + e.getMessage());
            try { sendError(client, OpCode.RESPONSE_SUBSCRIBERS_LIST, "Fetch subscribers failed: " + safeMsg(e)); }
            catch (Exception ignored) {}
        }
    }
    /**
     * Sends list of all reservations for agent dashboard.
     */
    private void handleAgentReservationsList(ConnectionToClient client) {
        try {
            List<Reservation> rows = reservationDAO.getAllReservations();
            List<ReservationDTO> dtoList = new ArrayList<>();
            for (Reservation r : rows) {
                dtoList.add(toReservationDTO(r));
            }
            sendOk(client, OpCode.RESPONSE_AGENT_RESERVATIONS_LIST, dtoList);
        } catch (Exception e) {
            log("Error fetching reservations: " + e.getMessage());
            // Always reply, otherwise the staff screen waits forever
            try { sendError(client, OpCode.RESPONSE_AGENT_RESERVATIONS_LIST, "Fetch reservations failed: " + safeMsg(e)); }
            catch (Exception ignored) {}
        }
    }
    /**
     * Staff removes a waiting-list entry by ID (sets to CANCELED and releases reserved tables).
     */
    private void handleRemoveWaitingCustomer(Envelope req, ConnectionToClient client) {
        try {
            Object payload = req.getPayload();
            
            // Staff sends just the ID (Integer)
            int waitingId = -1;
            if (payload instanceof Integer i) waitingId = i;
            else if (payload instanceof String s) {
                try { waitingId = Integer.parseInt(s.trim()); } catch (NumberFormatException ignored) {}
            }

            if (waitingId <= 0) {
                sendOk(client, OpCode.RESPONSE_WAITING_REMOVE, "Invalid waiting ID.");
                return;
            }

            // Call the new DAO method
            boolean success = DataBase.dao.WaitingListDAO.cancelWaitingById(waitingId);

            if (success) {
                sendOk(client, OpCode.RESPONSE_WAITING_REMOVE, "Customer removed from waiting list.");
            } else {
                sendOk(client, OpCode.RESPONSE_WAITING_REMOVE, "Could not remove (already arrived or not found).");
            }

        } catch (Exception e) {
            try {
                sendOk(client, OpCode.RESPONSE_WAITING_REMOVE, "Server error: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }
    /**
     * Returns waiting list items for agent view (WAITING/ASSIGNED).
     */
    private void handleGetWaitingList(ConnectionToClient client) {
        try {
            List<WaitingListDTO> list = WaitingListDAO.getAllWaitingList();
            sendOk(client, OpCode.RESPONSE_WAITING_LIST, list);
        } catch (Exception e) {
            log("Error fetching waiting list: " + e.getMessage());
            try {
                sendError(client, OpCode.RESPONSE_WAITING_LIST, "Fetch waiting list failed: " + safeMsg(e));
            } catch (Exception ignored) {}
        }
    }

    /* ==================== Helpers ==================== */
    /**
     * Converts {@link Reservation} entity to ReservationDTO using reflection,
     * to remain compatible with different DTO versions.
     *
     * @param r reservation entity
     * @return DTO object or null on failure
     */
    private static ReservationDTO toReservationDTO(Reservation r) {
        Timestamp resTs = r.getReservationTime();
        Timestamp expTs = r.getExpiryTime();
        String resTimeStr = (resTs == null) ? "-" : resTs.toLocalDateTime().toString().replace('T', ' ');
        String expTimeStr = (expTs == null) ? "-" : expTs.toLocalDateTime().toString().replace('T', ' ');

        return new ReservationDTO(
                r.getReservationId(),
                r.getConfirmationCode(),
                resTimeStr,
                expTimeStr,
                r.getNumOfCustomers(),
                r.getStatus());
    }
    /**
     * Finds up to 5 alternative reservation timestamps around a requested time.
     * Filters by:
     * <ul>
     *   <li>minute alignment (00/30)</li>
     *   <li>not in the past</li>
     *   <li>not beyond 1 month</li>
     *   <li>opening hours window</li>
     *   <li>capacity availability</li>
     * </ul>
     *
     * @param requested requested timestamp
     * @param numCustomers group size
     * @return list of candidate timestamps
     * @throws Exception DAO checks may throw
     */
    private List<Timestamp> findAlternativeTimes(Timestamp requested, int numCustomers) throws Exception {
        List<Timestamp> alternatives = new ArrayList<>();
        int[] offsets = { -120 ,-90, -60, -30, 30, 60, 90 ,120};

        for (int minutes : offsets) {
            Timestamp candidate = Timestamp.valueOf(requested.toLocalDateTime().plusMinutes(minutes));
            
            int m = candidate.toLocalDateTime().getMinute();
            if (m != 0 && m != 30) continue;
            
            if (candidate.before(new Timestamp(System.currentTimeMillis()))) continue;

            if (candidate.after(Timestamp.valueOf(java.time.LocalDateTime.now().plusMonths(1)))) continue;
            
            if (!OpeningHoursDAO.isOpenForReservation(candidate, 120)) continue;

            if (ReservationDAO.canFitAtTime(candidate, numCustomers)) {
                alternatives.add(candidate);
            }
            if (alternatives.size() == 5) break;
        }
        return alternatives;
    }
    
    /**
     * Helper method to send cancellation notifications (Email & SMS) to affected customers.
     * <p>
     * Iterates through the list of canceled reservations and sends a message explaining
     * the cancellation due to a change in opening hours.
     * </p>
     * <p>
     * <b>Logic:</b>
     * <ul>
     * <li>If the customer is a <b>Subscriber</b> (has a username), fresh contact details are fetched from the database.</li>
     * <li>If the customer is a <b>Guest</b>, the email/phone stored in the reservation is used.</li>
     * <li>Exceptions are caught individually to ensure that a failure to notify one customer does not stop the loop.</li>
     * </ul>
     * </p>
     *
     * @param canceledList A list of string arrays, where each array contains: {@code [username, guestEmail, guestPhone]}.
     * @param date A label representing the date or day affected (e.g., "2025-01-20" or "Every Monday").
     */
    private void notifyCanceledCustomers(List<String[]> canceledList, String date) {
        if (canceledList == null || canceledList.isEmpty()) return;

        log(">>> Notifying " + canceledList.size() + " customers of cancellation...");

        for (String[] row : canceledList) {
            String username = row[0];
            String email = row[1];
            String phone = row[2];

            // 1. Resolve Contact Info (Subscriber vs Guest)
            try {
                if (username != null && !username.isBlank()) {
                    // It's a subscriber, fetch fresh info from DB
                    String subEmail = DataBase.dao.SubscriberDAO.getEmailByUsername(username);
                    String subPhone = DataBase.dao.SubscriberDAO.getPhoneByUsername(username);
                    
                    if (subEmail != null && !subEmail.isBlank()) email = subEmail;
                    if (subPhone != null && !subPhone.isBlank()) phone = subPhone;
                }
            } catch (Exception e) {
                System.out.println("Error fetching subscriber info for notification: " + e.getMessage());
            }

            // 2. Send Email
            if (email != null && !email.isBlank()) {
                try {
                    EmailService.sendCancellationNotification(email, date, "Change in Restaurant Opening Hours");
                } catch (Exception e) {
                    System.out.println("Failed to email " + email);
                }
            }

            // 3. Send SMS
            if (phone != null && !phone.isBlank()) {
                try {
                    EmailService.sendSMSCancellation(phone, date);
                } catch (Exception e) {
                    System.out.println("Failed to SMS " + phone);
                }
            }
        }
    }
}