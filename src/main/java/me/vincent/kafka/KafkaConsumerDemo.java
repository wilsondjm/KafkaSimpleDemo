package me.vincent.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerDemo implements Runnable{

    private final KafkaConsumer consumer;

    public KafkaConsumerDemo(String topic){
        Properties properties = new Properties();
        //1、配置集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.18:9092, 10.0.0.20:9092, 10.0.0.21:9092");
        //2、配置Consumer的Group名字
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "VHTestGroup");
        //optional, 关闭自动应答
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //3、设置message的key的序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        //4、设置message的value的序列化类
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //5、设置获取的offset
        consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(topic));

    }

    @Override
    public void run() {
        while(true){
            ConsumerRecords<Integer, String> records =  consumer.poll(2000);
            for (Iterator<ConsumerRecord<Integer, String>> it = records.iterator(); it.hasNext(); ) {
                ConsumerRecord<Integer, String> record = it.next();
                System.out.println(String.format("Message received: topic[%s] - partition[%d] - key[%d] - value[%s]", record.topic(), record.partition(), record.key(), record.value()));
                consumer.commitAsync();
            }

        }
    }

    public static void main(String[] args){
        KafkaConsumerDemo kafkaConsumerDemo = new KafkaConsumerDemo("TestTopic1");
        ExecutorService service = Executors.newCachedThreadPool();
        service.submit(kafkaConsumerDemo);
    }
}
