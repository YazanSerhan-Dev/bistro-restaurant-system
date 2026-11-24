package Server;

import java.nio.charset.StandardCharsets;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Small wrapper around one Paho MQTT client used to publish retained stats messages.
 * <ul>
 *   <li>One {@link MqttClient} is created and reused (reconnects never leak old clients).</li>
 *   <li>{@link MemoryPersistence} is used, so no {@code bistro-server-tcp...} lock folder is
 *       written into the working directory.</li>
 *   <li>Paho's automatic reconnect takes over after the first successful connect.</li>
 * </ul>
 */
public class MqttBridge {
    private final String brokerUrl;
    private final String clientId;
    private MqttClient client;

    public MqttBridge(String brokerUrl, String clientId) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
    }

    /** Connects (or re-connects) the single client. Safe to call repeatedly. */
    public synchronized void connect() throws MqttException {
        if (client == null) {
            client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        }
        if (client.isConnected()) return;

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setConnectionTimeout(5);
        opts.setKeepAliveInterval(20);
        client.connect(opts);
        System.out.println("[MQTT] Connected to " + brokerUrl);
    }

    public synchronized boolean isConnected() {
        return client != null && client.isConnected();
    }

    public synchronized void publishRetained(String topic, String payloadJson) throws MqttException {
        if (!isConnected()) return;
        MqttMessage msg = new MqttMessage(payloadJson.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        msg.setRetained(true);
        client.publish(topic, msg);
    }

    /** Disconnects and releases the client threads (called when the server stops). */
    public synchronized void close() {
        if (client == null) return;
        try {
            if (client.isConnected()) client.disconnect(2000);
        } catch (Exception ignored) {
        }
        try {
            client.close();
        } catch (Exception ignored) {
        }
        client = null;
    }
}
