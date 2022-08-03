import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ExactPartitionConsumer {
    private final static Logger logger = LoggerFactory.getLogger(ExactPartitionConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "nit-dev-server:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        int PARTITION_NUMBER = 0;

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME,PARTITION_NUMBER)));//특정 파티션 할당

        Set<TopicPartition> assginedTopicPartition = consumer.assignment();//컨슈머에 할당된 토픽 및 파티션 정보
        logger.info("{}",assginedTopicPartition);

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
            for (ConsumerRecord<String, String> record:records){
                logger.info("{}", record);
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null));
                consumer.commitSync(currentOffsets);
            }
        }
    }
}
