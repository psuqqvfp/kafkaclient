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
        props.put("group.id", "mygroup3");
        props.put("auto.offset.reset", "earliest");//latest earliest none
        props.put("enable.auto.commit", "false");
//        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public static void main(String[] args) throws InterruptedException {
        consumer("1234567");
//        commitOffset("1234567",6);
    }

    public static  void getTopics(){
        KafkaConsumer<String, String> consumer = getConsumer();
        Map<String, List<PartitionInfo>> topics =  consumer.listTopics();
        for(String key : topics.keySet()){
            System.out.println(topics.get(key));
        }
    }

    public static  void getLastestOffset(String topic){
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(100);
        System.out.println(records.count());
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            long earestOffset = partitionRecords.get(1).offset();
            System.out.println("partition:"+partition+" lastOffset:"+lastOffset+" earestOffset:"+earestOffset);
        }

    }

    public static void commitOffset(String topic,int offset){
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList(topic));
        TopicPartition partition= new TopicPartition(topic,0);
        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)));
    }

    public static void consumer(String topic){
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    long earestOffset = partitionRecords.get(0).offset();
                    System.out.println("partition:"+partition+" lastestOffset:"+lastOffset+" earliestOffset:"+earestOffset);
                    //consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
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
