package Server;

/**
 * Entry point for running the Bistro server without the JavaFX window (e.g. on a Linux box via systemd).
 * <p>
 * Usage: {@code java -jar ServerHeadless.jar [--client-port 5555]}
 * <p>
 * The server listens for Java clients on the client port (default 5555). It also connects OUT to the
 * MQTT broker configured in {@code server.properties} ({@code mqtt.url}, default tcp://localhost:1883);
 * no MQTT port is opened by this process.
 */
public class HeadlessMain {

    public static void main(String[] args) {
        ServerConfig.applyTimeZone(); // must happen before any date/time work

        int clientPort = 5555;

        // optional args: --client-port 5555
        for (int i = 0; i < args.length - 1; i++) {
            if ("--client-port".equals(args[i])) {
                try {
                    clientPort = Integer.parseInt(args[i + 1].trim());
                } catch (NumberFormatException e) {
                    System.err.println("[HEADLESS] Invalid --client-port value: " + args[i + 1]);
                    System.exit(2);
                }
            }
        }

        try {
            System.out.println("[HEADLESS] Starting BistroServer on client port " + clientPort);
            BistroServer server = ServerLauncher.start(clientPort, null); // controller=null => logs go to console

            // Ctrl+C / systemctl stop: close connections, stop jobs, MQTT and the DB pool cleanly
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[HEADLESS] Shutting down...");
                try { server.close(); } catch (Exception ignored) {}
                BackgroundJobs.stop();
                DataBase.MySQLConnectionPool.shutdownIfCreated();
            }, "bistro-shutdown"));

            System.out.println("[HEADLESS] Server is running. Press Ctrl+C to stop.");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
