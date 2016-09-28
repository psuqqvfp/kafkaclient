package kafka.producter;

import kafka.javaapi.producer.Producer;
import kafka.model.KeyWord;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * User: Administrator
 * Date: 2016/7/13-14:16
 */
public class KafkaProducter {

    private final static Producer<String, String> producer;
    public final static String TOPIC = "testa";

    static {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.56.92:9092");
      //  props.put("zk.connect", "192.168.1.160:2001,192.168.1.168:2001");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks","1");
        props.put("kafka.client.id","topic1");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }

   static void produce() {
       List<KeyedMessage<String,String>> list = new ArrayList<KeyedMessage<String,String>>();
       for(int i=0;i<50;i++){
           KeyWord word = new KeyWord();
           word.setId(i+"");
           word.setData("data");
           word.setKeyword("key");
           word.setUser("admin");
           KeyedMessage<String,String> msg = new KeyedMessage(TOPIC, word.toString());
           list.add(msg);
       }
       producer.send(list);
       System.out.println("send   out");
       producer.close();
    }
    //http://www.open-open.com/lib/view/open1412991579999.html
    public static void main( String[] args )
    {
        produce();
    }
}
