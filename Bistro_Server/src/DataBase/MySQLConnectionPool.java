package DataBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import Server.ServerConfig;

/**
 * Singleton MySQL connection pool.
 * Reuses connections, validates them before reuse and periodically closes idle ones.
 * <p>
 * DB settings come from {@code server.properties} in the working directory (git-ignored):
 * {@code db.host, db.port, db.name, db.user, db.password}. There is intentionally no real
 * password in the source code; see {@code deployment/linux/config/server.properties.example}.
 */
public class MySQLConnectionPool {

	// -------- DB CONFIG (from server.properties) --------
	private static final String URL;
	private static final String USER;
	private static final String PASSWORD;
	private static final String TIME_ZONE;

	static {
	    String host = ServerConfig.get("db.host", "localhost");
	    String port = ServerConfig.get("db.port", "3306");
	    String db   = ServerConfig.get("db.name", "Bistro");

	    USER = ServerConfig.get("db.user", "CHANGE_ME");
	    PASSWORD = ServerConfig.get("db.password", "CHANGE_ME");
	    TIME_ZONE = ServerConfig.timeZoneId();

	    URL = "jdbc:mysql://" + host + ":" + port + "/" + db
	        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=" + TIME_ZONE;

	    if ("CHANGE_ME".equals(USER)) {
	        System.out.println("[DB] WARNING: db.user/db.password not set. Create server.properties "
	                + "(see deployment/linux/config/server.properties.example).");
	    }
	}

    // ---------- POOL CONFIG ----------
    /** Maximum number of idle connections kept in the pool. */
    private static final int  MAX_POOL_SIZE      = 10;
    /** Idle timeout – how long a connection may sit unused before cleanup (ms). */
    private static final long MAX_IDLE_MILLIS    = 30_000L;
    /** How often the cleanup task runs (seconds). */
    private static final long CLEANUP_PERIOD_SEC = 10L;
    /** Seconds to wait when validating a connection before reuse. */
    private static final int  VALIDATE_TIMEOUT_SEC = 2;

    // ---------- SINGLETON INSTANCE ----------
    private static MySQLConnectionPool instance;

    /**
     * Returns the single instance of the pool (lazy-loaded Singleton).
     */
    public static synchronized MySQLConnectionPool getInstance() {
        if (instance == null) {
            instance = new MySQLConnectionPool();
        }
        return instance;
    }

    /** Shuts the pool down if it was ever created (safe to call on server exit). */
    public static synchronized void shutdownIfCreated() {
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    // ---------- INTERNAL STATE ----------
    /** Queue of pooled connections that are currently free to use. */
    private final BlockingQueue<PooledConnection> pool;
    /** Background task that periodically removes idle connections. */
    private final ScheduledExecutorService cleaner;

    /**
     * Private constructor – only getInstance() can create the pool.
     * Initializes the queue and schedules the periodic cleanup job.
     */
    private MySQLConnectionPool() {
        pool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);

        // Daemon thread: the pool must never keep the JVM alive after the server window is closed
        cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "DB-pool-cleaner");
            t.setDaemon(true);
            return t;
        });
        cleaner.scheduleAtFixedRate(
                this::cleanupIdleConnections,
                CLEANUP_PERIOD_SEC,
                CLEANUP_PERIOD_SEC,
                TimeUnit.SECONDS
        );
    }

    /**
     * Gets a pooled connection from the queue (after checking it is still alive),
     * or creates a new one if needed.
     *
     * @return a PooledConnection ready to use (wrapped JDBC Connection)
     */
    public PooledConnection getConnection() {
        PooledConnection pConn;
        while ((pConn = pool.poll()) != null) {
            if (isUsable(pConn)) {
                pConn.touch();
                return pConn;
            }
            pConn.closePhysicalConnection(); // dead (DB restart / network) -> drop it
        }

        try {
            Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            applySessionTimeZone(conn);
            return new PooledConnection(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create DB connection", e);
        }
    }

    private static boolean isUsable(PooledConnection pConn) {
        try {
            Connection c = pConn.getConnection();
            return !c.isClosed() && c.getAutoCommit() && c.isValid(VALIDATE_TIMEOUT_SEC);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Makes MySQL's NOW()/CURDATE() use the restaurant time zone, the same zone as the JVM.
     * An offset (e.g. +02:00) is used because named zones need MySQL's time zone tables.
     * Connections are recycled every few seconds of idleness, so DST changes are picked up.
     */
    private static void applySessionTimeZone(Connection conn) {
        try (Statement st = conn.createStatement()) {
            ZoneOffset offset = ZoneId.of(TIME_ZONE).getRules().getOffset(Instant.now());
            String id = offset.getId().equals("Z") ? "+00:00" : offset.getId();
            st.execute("SET time_zone = '" + id + "'");
        } catch (Exception e) {
            System.out.println("[DB] Could not set session time zone: " + e.getMessage());
        }
    }

    /**
     * Returns a pooled connection back to the pool.
     * If the pool is already full, or the connection was left inside a transaction,
     * the underlying JDBC connection is closed instead.
     */
    public void releaseConnection(PooledConnection pConn) {
        if (pConn == null) {
            return;
        }

        pConn.touch();

        boolean offered = isReturnable(pConn) && pool.offer(pConn);
        if (!offered) {
            pConn.closePhysicalConnection();
        }
    }

    /** Cheap local check (no DB round trip): open and not left in the middle of a transaction. */
    private static boolean isReturnable(PooledConnection pConn) {
        try {
            Connection c = pConn.getConnection();
            return !c.isClosed() && c.getAutoCommit();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Periodic cleanup: closes connections that have been idle for too long.
     * Runs automatically according to CLEANUP_PERIOD_SEC.
     */
    private void cleanupIdleConnections() {
        long now = System.currentTimeMillis();

        Iterator<PooledConnection> it = pool.iterator();
        while (it.hasNext()) {
            PooledConnection pConn = it.next();
            if (now - pConn.getLastUsed() > MAX_IDLE_MILLIS) {
                it.remove();
                pConn.closePhysicalConnection();
            }
        }
    }

    /**
     * Shuts down the pool: stops the cleaner thread and closes all pooled connections.
     */
    public void shutdown() {
        cleaner.shutdownNow();
        PooledConnection pConn;
        while ((pConn = pool.poll()) != null) {
            pConn.closePhysicalConnection();
        }
    }
}
