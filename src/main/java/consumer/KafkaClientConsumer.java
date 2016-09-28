package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Administrator on 2016/7/31.
 */
public class KafkaClientConsumer {

    public static KafkaConsumer<String, String> getConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "slave1:9092,slave2:9092");
        props.put("group.id", "mygroup");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public static void main(String[] args) throws InterruptedException {
//        getTopics();
//        consumer("201609288");
        seek("201609288");
    }

    public static void seek(String topic){
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                System.out.println(records.partitions());
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
                Thread.sleep(3000);
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }

    public static  void getTopics(){
        KafkaConsumer<String, String> consumer = getConsumer();
        Map<String, List<PartitionInfo>> topics =  consumer.listTopics();
        for(String key : topics.keySet()){
            System.out.println(topics.get(key));
        }
    }

    public static void consumer(String topic){
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                System.out.println(records.partitions());
                for (TopicPartition partition : records.partitions()) {
                    System.out.println(partition);
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
                Thread.sleep(3000);
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }
}
