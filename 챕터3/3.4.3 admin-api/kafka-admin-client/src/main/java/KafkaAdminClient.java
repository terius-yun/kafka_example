import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClient {
    private final static Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "nit-dev-server:9092");

        AdminClient admin = AdminClient.create(configs);
        logger.info("== Get broker information");
        for(Node node : admin.describeCluster().nodes().get()){
            logger.info("node : {}",node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config)-> {
                config.entries().forEach(configEntry -> logger.info(configEntry.name() + " = "+ configEntry.value()));
            });
        }
        Map<String, TopicDescription> topicInfomation = admin.describeTopics(Collections.singletonList("test")).allTopicNames().get();
        logger.info("=== topic info =====");
        logger.info("{}", topicInfomation);
        admin.close();
    }
}
