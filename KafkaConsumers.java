package cn.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
*  git 修改版本4 .0
*  5.0
 *
 * 此方法为kafka在IDEA中的消费者API使用情况：
 * 开启此消费者后，需要在 linux控制台 或者 开启IDEA中kafka的producer程序，
 * 开启之后：会在此消费者程序控制台上输出消费的信息
 */
public class KafkaConsumers {
    /**
     * consumer 消费者需要的配置信息
     * BOOTSTRAP_SERVERS_CONFIG  指定zookeeper集群
     * KEY_DESERIALIZER_CLASS_CONFIG  key值的反序列化
     * VALUE_DESERIALIZER_CLASS_CONFIG  value的反序列化
     * ENABLE_AUTO_COMMIT_CONFIG  自动提交，默认 true
     * GROUP_ID_CONFIG  消费者组
     * @param args
     */

    public static void main(String[] args) {

        Properties properties = new Properties();

        //指定zookeeper集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
        /**
         * 注意：这里 key ，value 采用的是反序列化器 Deserializer ，需要和生产者的序列化器 Serializer 区分开
         */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        //把自动提交关闭
       // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"1205");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //订阅topic
        consumer.subscribe(Arrays.asList("first"));

        //调用poll方法
        while (true){
            //停留100毫秒拉取数据一批数据
            ConsumerRecords<String,String> records = consumer.poll(100);
            /*
            * 遍历拉取的数据
            * */
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("record offset="+record.offset()+"record value="+record.value());
            }
            //consumer.commitAsync(); // 同步提交，主要是为了提交offset位置
            //consumer.commitSync(); // 异步提交，主要是为了提交offset位置
        }

    }
}
