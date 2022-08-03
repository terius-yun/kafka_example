import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ShutdownHookConsumer {
    private final static Logger logger = LoggerFactory.getLogger(ShutdownHookConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "nit-dev-server:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {

        ShutdownThread shutdownThread = new ShutdownThread();
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        shutdownThread.setConsumer(consumer);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record: records){
                    logger.info("{}",record);
                }
            }
        }catch (WakeupException e){
            logger.warn("Wackup consumer");
        }finally {
            consumer.close();
        }

    }
}
