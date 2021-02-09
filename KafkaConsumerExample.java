package myapps;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerExample {
    private static String myTopic = "ziumSeminar";
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "192.168.0.101:9978,192.168.0.102:9978,192.168.0.104:9978");

        conf.setProperty("group.id","ziumConsumer");
        conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("consumer.id", "0");
        conf.setProperty("client.id", "ziumConsumer");
        conf.setProperty("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Collections.singleton(myTopic));

        for(int i=0; i<9999; i++){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<Integer, String> record: records){
                String msg = String.format("key : %d , value : %s", record.key(), record.value() );
                System.out.println(msg);

                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
                Map<TopicPartition, OffsetAndMetadata> info = Collections.singletonMap(topicPartition, offsetAndMetadata);
                consumer.commitSync(info);
            }
            try{
                Thread.sleep(10);
            }catch (InterruptedException error){
                error.printStackTrace();
            }
        }
    consumer.close();
    }
}
