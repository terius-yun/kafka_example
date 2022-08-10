import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterProcessor implements Processor {
    private ProcessorContext context;
    private static Logger logger = LoggerFactory.getLogger(FilterProcessor.class);

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Object key, Object value) {
        logger.info((String)value);
        if(value.toString().length() > 5){
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {
    }
}