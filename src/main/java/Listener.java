import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

/**
 * @author ebruno
 */
public class Listener implements IMqttMessageListener {
    public static int qos           = 1;
    public static String server     = "localhost";
    public static String port       = "1883";
    public static String broker     = "tcp://localhost:1883";
    public static String clientId   = "mqtt_listener";
    public static String topic      = "topic1";

    public static MemoryPersistence persistence = new MemoryPersistence();
    public static MqttClient client;

    public static void main(String[] args) {
        // See if a topic names was provided on the command line
        if ( args.length == 1) {
            topic = args[0];
        }
        else {
            for ( int i = 0; i < args.length; i++ ) {
                switch ( args[i] ) {
                    case "server":
                        server = args[++i];
                        break;
                    case "port":
                        port = args[++i];
                        break;
                    case "topic":
                        topic = args[++i];
                        break;
                }

            }
        }

        broker = "tcp://"+server+":"+port;

        Listener listener = new Listener();

        try {
            listener.connect();
            listener.listen(topic);
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public Listener() {
    }
    
    public void connect() throws MqttException {
        try {
            System.out.println("Connecting to MQTT broker: "+broker);

            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(false);

            client = new MqttClient(broker, clientId, persistence);
            client.connect(connOpts);

            System.out.println("Connected");
        }
        catch ( MqttException me ) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
            throw me;
        }
    }

    public void disconnect() {
        try {
            client.disconnect();
            System.out.println("Disconnected");
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    public void listen(String topic) throws Exception {
        try {
            System.out.println("Subscribing to topic " + topic);
                    
            MqttSubscription sub =
                    new MqttSubscription(topic, qos);

            IMqttToken token = client.subscribe(
                    new MqttSubscription[] { sub }, 
                    new IMqttMessageListener[] { this });
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        String messageTxt = new String( mqttMessage.getPayload() );
        System.out.println("Message on " + topic + ": '" + messageTxt + "'");
       
        MqttProperties props = mqttMessage.getProperties();	
        String responseTopic = props.getResponseTopic(); 
        if ( responseTopic != null ) {
            System.out.println("--Response topic: " + responseTopic);
            String corrData = new String(props.getCorrelationData());

            MqttMessage response = new MqttMessage();
	    props = new MqttProperties();
	    props.setCorrelationData(corrData.getBytes());
            String content = "Got message with correlation data " + corrData;
            response.setPayload(content.getBytes());
	    response.setProperties(props);
            client.publish(responseTopic, response);
        }
    }
}
