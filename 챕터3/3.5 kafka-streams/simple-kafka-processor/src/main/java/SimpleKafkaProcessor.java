import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor{

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "nit-dev-server:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", STREAM_LOG)
                .addProcessor("Process", ()-> new FilterProcessor(), "Source")
                .addSink("Sink",STREAM_LOG_FILTER, "Process");

        KafkaStreams streaming = new KafkaStreams(topology, prop);
        streaming.start();
    }
}
