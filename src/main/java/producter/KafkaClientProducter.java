package producter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTimestampException;

import java.util.Properties;

/**
 * Created by Administrator on 2016/7/31.
 */
public class KafkaClientProducter {
    public static void main(String[] args) {
        String topicName = "1234567";
        Properties props = new Properties();
        props.put("bootstrap.servers", "slave1:9092,slave2:9092");
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);

        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i <10; i++)
            producer.send(new ProducerRecord<String, String>(topicName, "message No:"+i));
        producer.close();
        System.out.println("send out");
    }
}
