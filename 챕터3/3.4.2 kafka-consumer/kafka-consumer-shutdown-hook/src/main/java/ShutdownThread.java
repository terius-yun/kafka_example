import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownThread extends Thread{
    private final static Logger logger = LoggerFactory.getLogger(ShutdownThread.class);

    private KafkaConsumer consumer;
    public void setConsumer(KafkaConsumer<String, String> consumer){
        this.consumer = consumer;
    }

    public void run(){
        logger.info("Shutdown hook");
        consumer.wakeup();
    }
}
