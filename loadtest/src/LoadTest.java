import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import common.Envelope;
import common.KryoMessage;
import common.KryoUtil;
import common.OpCode;
import common.dto.LoginRequestDTO;
import common.dto.LoginResponseDTO;
import common.dto.MakeReservationRequestDTO;
import common.dto.MakeReservationResponseDTO;
import common.dto.TerminalValidateResponseDTO;
import ocsf.client.AbstractClient;

/**
 * Load generator for the Bistro server.
 * <p>
 * Every virtual client opens its OWN OCSF connection and sends real Kryo-encoded {@link Envelope}
 * requests - exactly what the JavaFX client sends - so the numbers measure the real server code,
 * the real database queries and the real network protocol (no HTTP stand-in).
 * <p>
 * Workload per iteration (a customer browsing and booking):
 * <ol>
 *   <li>today's opening hours</li>
 *   <li>available times for a random date (1-28 days ahead) and party size (1-4)</li>
 *   <li>availability check for one of those times</li>
 *   <li>30% of iterations: make the reservation, then cancel it (keeps availability steady)</li>
 *   <li>"my reservations" list</li>
 *   <li>terminal code validation</li>
 * </ol>
 * Half of the clients log in as subscribers first (PBKDF2 password check); the rest are guests.
 * Closed loop, no think time: each client sends its next request as soon as the previous reply arrives.
 * <p>
 * A request counts as an ERROR if it times out, the connection fails, or the server answers with an
 * error envelope / "Server error". A correct business answer ("no seats, try 19:30") is a success.
 *
 * Usage: java LoadTest --clients 25 --duration 60 --warmup 10 [--rampup 5] [--host 127.0.0.1]
 *                      [--port 5555] [--label baseline] [--out results/run.csv]
 * <p>
 * --rampup spreads the connection attempts over N seconds (JMeter-style). With 0 every client
 * connects at the same instant, which also tests how the server copes with a connection burst.
 */
public class LoadTest {

    // ------------------------------------------------------------------ connection

    /** One virtual client = one real OCSF connection. Replies are matched in order (one request in flight). */
    static final class BistroConnection extends AbstractClient {
        private final BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();

        BistroConnection(String host, int port) { super(host, port); }

        @Override
        protected void handleMessageFromServer(Object msg) { inbox.offer(msg); }

        Envelope call(OpCode op, Object payload, long timeoutMs) throws Exception {
            inbox.clear();
            sendToServer(new KryoMessage("ENVELOPE", KryoUtil.toBytes(Envelope.request(op, payload))));
            Object reply = inbox.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (reply == null) throw new Exception("timeout after " + timeoutMs + " ms");
            if (!(reply instanceof KryoMessage km)) throw new Exception("unexpected reply " + reply.getClass());
            return KryoUtil.fromBytes(km.getPayload());
        }
    }

    // ------------------------------------------------------------------ samples

    record Sample(String op, long startNs, long latencyNs, boolean ok, String error) {}

    static final ConcurrentLinkedQueue<Sample> SAMPLES = new ConcurrentLinkedQueue<>();
    static final long TIMEOUT_MS = 30_000;

    // ------------------------------------------------------------------ main

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String host = a.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(a.getOrDefault("port", "5555"));
        int clients = Integer.parseInt(a.getOrDefault("clients", "10"));
        int durationSec = Integer.parseInt(a.getOrDefault("duration", "60"));
        int warmupSec = Integer.parseInt(a.getOrDefault("warmup", "10"));
        int rampupSec = Integer.parseInt(a.getOrDefault("rampup", "0"));
        String label = a.getOrDefault("label", "baseline");
        String out = a.get("out");

        System.out.printf("[LoadTest] %s: %d clients, %ds ramp-up, %ds warm-up + %ds measured, server %s:%d%n",
                label, clients, rampupSec, warmupSec, durationSec, host, port);

        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch go = new CountDownLatch(1);
        long[] window = new long[2]; // measured window [start, end) in nanoTime
        List<Thread> threads = new ArrayList<>();
        List<String> connectErrors = Collections.synchronizedList(new ArrayList<>());

        // Ramp-up (like JMeter): client i connects at i * rampup / clients seconds, instead of all at once
        for (int i = 0; i < clients; i++) {
            final int id = i;
            final long delayMs = clients <= 1 ? 0 : (long) i * rampupSec * 1000L / clients;
            Thread t = new Thread(() -> {
                try { Thread.sleep(delayMs); } catch (InterruptedException e) { return; }
                runClient(id, host, port, ready, go, window, connectErrors);
            }, "vuser-" + i);
            t.start();
            threads.add(t);
        }

        ready.await(60L + rampupSec, TimeUnit.SECONDS);
        long t0 = System.nanoTime();
        window[0] = t0 + TimeUnit.SECONDS.toNanos(warmupSec);
        window[1] = window[0] + TimeUnit.SECONDS.toNanos(durationSec);
        go.countDown();

        for (Thread t : threads) t.join(TimeUnit.SECONDS.toMillis(warmupSec + durationSec) + TIMEOUT_MS + 10_000);

        report(label, clients, durationSec, window, connectErrors, out);
        System.exit(0);
    }

    // ------------------------------------------------------------------ virtual client

    static void runClient(int id, String host, int port, CountDownLatch ready, CountDownLatch go,
                          long[] window, List<String> connectErrors) {
        Random rnd = new Random(1000L + id);
        BistroConnection c = new BistroConnection(host, port);
        boolean subscriber = (id % 2 == 0);
        String guestEmail = "guest" + id + "@loadtest.local";
        String guestPhone = String.format("054%07d", id);
        String user = "load" + (id % 10 + 1);
        String lastCode = null;

        try {
            c.openConnection();
        } catch (Exception e) {
            connectErrors.add("vuser-" + id + ": " + e.getMessage());
            ready.countDown();
            return;
        }
        ready.countDown();
        try { go.await(); } catch (InterruptedException e) { return; }

        try {
            if (subscriber) {
                timed(c, "LOGIN_SUBSCRIBER", OpCode.REQUEST_LOGIN_SUBSCRIBER,
                        new LoginRequestDTO(user, "LoadPass1!"), window);
            }

            while (System.nanoTime() < window[1]) {
                if (!c.isConnected()) {
                    // The server dropped this client: count it once and stop (a real user would be gone too)
                    long now = System.nanoTime();
                    if (now >= window[0]) SAMPLES.add(new Sample("CONNECTION_LOST", now, 0, false, "connection lost"));
                    break;
                }
                LocalDate date = LocalDate.now().plusDays(1 + rnd.nextInt(28));
                int party = 1 + rnd.nextInt(4);

                timed(c, "TODAY_HOURS", OpCode.REQUEST_TODAY_HOURS, null, window);

                Envelope times = timed(c, "GET_AVAILABLE_TIMES", OpCode.REQUEST_GET_AVAILABLE_TIMES,
                        new Object[] { date.toString(), party }, window);

                String slot = null;
                if (times != null && times.getPayload() instanceof List<?> list && !list.isEmpty()) {
                    slot = String.valueOf(list.get(rnd.nextInt(list.size())));
                }
                if (slot != null) {
                    Timestamp ts = Timestamp.valueOf(LocalDateTime.of(date, LocalTime.parse(slot)));
                    timed(c, "CHECK_AVAILABILITY", OpCode.REQUEST_CHECK_AVAILABILITY,
                            new MakeReservationRequestDTO(null, null, null, party, ts), window);

                    if (rnd.nextInt(100) < 30) {
                        MakeReservationRequestDTO req = subscriber
                                ? new MakeReservationRequestDTO(user, null, null, party, ts)
                                : new MakeReservationRequestDTO(null, guestPhone, guestEmail, party, ts);
                        Envelope made = timed(c, "MAKE_RESERVATION", OpCode.REQUEST_MAKE_RESERVATION, req, window);
                        if (made != null && made.getPayload() instanceof MakeReservationResponseDTO r
                                && r.isOk() && r.getConfirmationCode() != null) {
                            lastCode = r.getConfirmationCode();

                            // Cancel it again (remote cancel, with the same identity). This keeps the
                            // restaurant's availability steady during the whole run, so every run does the
                            // same mix of work no matter how fast the server is.
                            Object[] cancel = subscriber
                                    ? new Object[] { "SUBSCRIBER", user, null, null, r.getReservationId() }
                                    : new Object[] { "CUSTOMER", "", guestEmail, guestPhone, r.getReservationId() };
                            timed(c, "CANCEL_RESERVATION", OpCode.REQUEST_CANCEL_RESERVATION, cancel, window);
                        }
                    }
                }

                Object[] listPayload = subscriber
                        ? new Object[] { "SUBSCRIBER", user, null, null }
                        : new Object[] { "CUSTOMER", "", guestEmail, guestPhone };
                timed(c, "RESERVATIONS_LIST", OpCode.REQUEST_RESERVATIONS_LIST, listPayload, window);

                timed(c, "TERMINAL_VALIDATE_CODE", OpCode.REQUEST_TERMINAL_VALIDATE_CODE,
                        lastCode != null ? lastCode : "NOSUCHCODE", window);
            }
        } finally {
            try { c.closeConnection(); } catch (Exception ignored) {}
        }
    }

    /** Sends one request, times it, classifies it, and records it if it started inside the measured window. */
    static Envelope timed(BistroConnection c, String op, OpCode code, Object payload, long[] window) {
        long start = System.nanoTime();
        Envelope env = null;
        String error = null;
        try {
            env = c.call(code, payload, TIMEOUT_MS);
            error = classify(env);
        } catch (Exception e) {
            error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        }
        long latency = System.nanoTime() - start;
        if (start >= window[0] && start < window[1]) {
            SAMPLES.add(new Sample(op, start, latency, error == null, error));
        }
        return error == null ? env : null;
    }

    /** @return null if the server gave a correct answer, otherwise a short error description */
    static String classify(Envelope env) {
        if (env == null) return "no envelope";
        if (!env.isOk() || env.getOp() == OpCode.ERROR) {
            return "error envelope: " + env.getMessage();
        }
        Object p = env.getPayload();
        if (p instanceof String s && s.startsWith("Server error")) return s;
        if (env.getOp() == OpCode.RESPONSE_CANCEL_RESERVATION && p instanceof String s
                && !s.startsWith("Reservation cancelled")) return "cancel refused: " + s;
        if (p instanceof LoginResponseDTO l && !l.isOk()) return "login failed: " + l.getMessage();
        if (p instanceof MakeReservationResponseDTO r && !r.isOk()
                && r.getMessage() != null && r.getMessage().startsWith("Server error")) return r.getMessage();
        if (p instanceof TerminalValidateResponseDTO t
                && t.getMessage() != null && t.getMessage().startsWith("Server error")) return t.getMessage();
        return null;
    }

    // ------------------------------------------------------------------ report

    static void report(String label, int clients, int durationSec, long[] window,
                       List<String> connectErrors, String out) throws Exception {
        List<Sample> all = new ArrayList<>(SAMPLES);
        Map<String, List<Sample>> byOp = new TreeMap<>();
        for (Sample s : all) byOp.computeIfAbsent(s.op(), k -> new ArrayList<>()).add(s);

        double seconds = (window[1] - window[0]) / 1e9;
        Stats total = Stats.of(all, seconds);

        System.out.println();
        System.out.printf("%-24s %8s %7s %9s %9s %9s %9s %9s %10s%n",
                "operation", "requests", "errors", "avg ms", "p50 ms", "p95 ms", "p99 ms", "max ms", "req/s");
        for (var e : byOp.entrySet()) {
            Stats s = Stats.of(e.getValue(), seconds);
            System.out.printf("%-24s %8d %7d %9.1f %9.1f %9.1f %9.1f %9.1f %10.1f%n",
                    e.getKey(), s.count, s.errors, s.avg, s.p50, s.p95, s.p99, s.max, s.throughput);
        }
        System.out.printf("%-24s %8d %7d %9.1f %9.1f %9.1f %9.1f %9.1f %10.1f%n",
                "ALL", total.count, total.errors, total.avg, total.p50, total.p95, total.p99, total.max, total.throughput);
        System.out.printf("error rate: %.2f%%   connected clients: %d / %d   connect failures: %d%n",
                total.count == 0 ? 0.0 : 100.0 * total.errors / total.count,
                clients - connectErrors.size(), clients, connectErrors.size());

        Map<String, Integer> errorKinds = new HashMap<>();
        for (Sample s : all) if (!s.ok()) errorKinds.merge(s.error(), 1, Integer::sum);
        errorKinds.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(5)
                .forEach(e -> System.out.println("  error x" + e.getValue() + ": " + e.getKey()));
        connectErrors.stream().limit(3).forEach(e -> System.out.println("  connect: " + e));

        if (out != null) {
            Path p = Path.of(out);
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            boolean header = !Files.exists(p);
            try (PrintWriter w = new PrintWriter(new FileWriter(p.toFile(), true))) {
                if (header) {
                    w.println("label,clients,operation,requests,errors,error_rate_pct,avg_ms,p50_ms,p95_ms,p99_ms,max_ms,throughput_rps,connect_failures");
                }
                for (var e : byOp.entrySet()) writeRow(w, label, clients, e.getKey(), Stats.of(e.getValue(), seconds), connectErrors.size());
                writeRow(w, label, clients, "ALL", total, connectErrors.size());
            }
        }
    }

    static void writeRow(PrintWriter w, String label, int clients, String op, Stats s, int connectFailures) {
        w.printf(java.util.Locale.ROOT, "%s,%d,%s,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d%n",
                label, clients, op, s.count, s.errors, s.count == 0 ? 0.0 : 100.0 * s.errors / s.count,
                s.avg, s.p50, s.p95, s.p99, s.max, s.throughput, connectFailures);
    }

    static final class Stats {
        long count, errors;
        double avg, p50, p95, p99, max, throughput;

        static Stats of(List<Sample> samples, double seconds) {
            Stats s = new Stats();
            s.count = samples.size();
            if (s.count == 0) return s;
            long[] lat = new long[samples.size()];
            long sum = 0;
            for (int i = 0; i < lat.length; i++) {
                Sample x = samples.get(i);
                lat[i] = x.latencyNs();
                sum += lat[i];
                if (!x.ok()) s.errors++;
            }
            java.util.Arrays.sort(lat);
            s.avg = sum / (double) lat.length / 1e6;
            s.p50 = pct(lat, 50) / 1e6;
            s.p95 = pct(lat, 95) / 1e6;
            s.p99 = pct(lat, 99) / 1e6;
            s.max = lat[lat.length - 1] / 1e6;
            s.throughput = s.count / seconds;
            return s;
        }

        /** Nearest-rank percentile. */
        static long pct(long[] sorted, double p) {
            int rank = (int) Math.ceil(p / 100.0 * sorted.length);
            return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
        }
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) m.put(args[i].substring(2), args[i + 1]);
        }
        return m;
    }
}
