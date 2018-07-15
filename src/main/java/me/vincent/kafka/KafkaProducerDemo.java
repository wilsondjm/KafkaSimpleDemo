package me.vincent.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemo {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    private static final String TOPIC = "TestTopic1";

    private static final String BROKER_LIST = "10.0.0.18:9092,10.0.0.20:9092,10.0.0.21:9092";

    private static KafkaProducer<Integer, String> producer = null;

    static {
        producer = new KafkaProducer<Integer, String>(initProperties());
    }

    private static Properties initProperties(){
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerDemo");
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        return properties;
    }

    public static void main(String[] args){
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<Integer, String>(TOPIC, i, "Message_" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    LOG.info(String.format("Send Async, offset[%d] - partition[%d]", metadata.offset(), metadata.partition()));
                }
            });
            //sync
//            try {
//                RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(TOPIC, i, "Message_" + i)).get();
//                LOG.info(String.format("Send Async, offset[%d] - partition[%d]", metadata.offset(), metadata.partition()));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}
