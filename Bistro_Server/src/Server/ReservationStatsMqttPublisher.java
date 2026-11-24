package Server;

import DataBase.dao.ReservationDAO;
import DataBase.dao.VisitDAO;

/**
 * Periodically publishes today's reservation stats to an MQTT topic as a retained JSON message:
 * {@code {"today":N,"served":M}}.
 * <p>
 * A message is published only when the numbers change, plus a heartbeat once a minute
 * (the message is retained, so new subscribers always get the latest value).
 */
public class ReservationStatsMqttPublisher {

    /** Re-publish unchanged values at least this often (ms). */
    private static final long HEARTBEAT_MS = 60_000L;

    private final MqttBridge mqtt;
    private final String topic;
    private final long intervalMs;
    private Thread worker;
    private volatile boolean running = false;

    // last published values (to avoid spamming the broker and the console)
    private int lastToday = Integer.MIN_VALUE;
    private int lastServed = Integer.MIN_VALUE;
    private long lastPublishAt = 0L;

    public ReservationStatsMqttPublisher(MqttBridge mqtt, String topic, long intervalMs) {
        this.mqtt = mqtt;
        this.topic = topic;
        this.intervalMs = intervalMs;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        worker = new Thread(() -> {
            while (running) {
                try {
                    // First connect may have failed (broker started later): keep trying with the SAME client
                    if (!mqtt.isConnected()) {
                        try {
                            mqtt.connect();
                        } catch (Exception ignored) {
                        }
                    }

                    if (mqtt.isConnected()) {
                        int today = Math.max(0, ReservationDAO.countReservationsMadeToday());
                        int served = Math.max(0, VisitDAO.countReservationsServedToday());

                        long now = System.currentTimeMillis();
                        boolean changed = today != lastToday || served != lastServed;
                        if (changed || now - lastPublishAt >= HEARTBEAT_MS) {
                            String payload = "{\"today\":" + today + ",\"served\":" + served + "}";
                            mqtt.publishRetained(topic, payload);
                            lastToday = today;
                            lastServed = served;
                            lastPublishAt = now;
                            if (changed) System.out.println("[MQTT] Published: " + payload + " -> " + topic);
                        }
                    }

                } catch (Exception e) {
                    System.out.println("[MQTT] Publisher error: " + e.getMessage());
                }

                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "ReservationStatsMqttPublisher");

        worker.setDaemon(true);
        worker.start();
    }

    public synchronized void stop() {
        running = false;
        if (worker != null) worker.interrupt();
        worker = null;
    }
}
