package Server;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Reads optional runtime settings from {@code server.properties} in the server's working directory
 * (the same file that holds the DB settings; it is git-ignored). Every key has a safe default.
 * <p>
 * Keys used:
 * <ul>
 *   <li>{@code app.timezone}  - restaurant time zone, default {@code Asia/Jerusalem}</li>
 *   <li>{@code mqtt.enabled}  - {@code true}/{@code false}, default {@code true}</li>
 *   <li>{@code mqtt.url}      - broker URL, default {@code tcp://localhost:1883}</li>
 *   <li>{@code mqtt.clientId} - client id, default {@code bistro-server}</li>
 * </ul>
 */
public final class ServerConfig {

    private static final Properties PROPS = load();

    private ServerConfig() {}

    private static Properties load() {
        Properties p = new Properties();
        Path path = Path.of("server.properties");
        if (Files.isRegularFile(path)) {
            try (InputStream in = new FileInputStream(path.toFile())) {
                p.load(in);
            } catch (Exception e) {
                System.out.println("[CONFIG] Could not read server.properties: " + e.getMessage());
            }
        }
        return p;
    }

    /** Returns a trimmed value, or {@code def} if the key is missing/blank. */
    public static String get(String key, String def) {
        String v = PROPS.getProperty(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public static int getInt(String key, int def) {
        String v = PROPS.getProperty(key);
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            System.out.println("[CONFIG] Invalid number for " + key + ": " + v + " (using " + def + ")");
            return def;
        }
    }

    public static boolean getBoolean(String key, boolean def) {
        String v = PROPS.getProperty(key);
        return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
    }

    /** The restaurant time zone (falls back to Asia/Jerusalem if the configured id is invalid). */
    public static String timeZoneId() {
        String tz = get("app.timezone", "Asia/Jerusalem");
        try {
            ZoneId.of(tz);
            return tz;
        } catch (Exception e) {
            System.out.println("[CONFIG] Invalid app.timezone '" + tz + "', using Asia/Jerusalem");
            return "Asia/Jerusalem";
        }
    }

    /**
     * Makes the JVM use the restaurant time zone, so Java's "now" (1-hour rule, available slots,
     * reminders) matches MySQL's NOW() (the DB session is set to the same zone by the pool).
     * Call once at startup, before anything else.
     */
    public static void applyTimeZone() {
        String tz = timeZoneId();
        TimeZone.setDefault(TimeZone.getTimeZone(tz));
        System.out.println("[CONFIG] Time zone: " + tz);
    }
}
