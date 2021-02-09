package myapps;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerExample {
	
	private static String myTopic = "ziumSeminar";
	
	public static void main(String[] arg) {
		
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "192.168.0.101:9978,192.168.0.102:9978,192.168.0.104:9978");
		conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
//		conf.put("bootstrap.servers", "192.168.0.101:9978,192.168.0.102:9978,192.168.0.104:9978");
//		conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//      conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		KafkaProducer<Integer, String> prdc =  new KafkaProducer<>(conf);
		
		int key;
		String value;
		for(int i=0;i<50000;i++) {
			key = i;
			value = "CJM Msg :" + i;
			ProducerRecord<Integer, String> record = new ProducerRecord<>(myTopic, key, value);
			
			prdc.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if(metadata != null) {
						String data = String.format("Success partition:%d, offset:%d", metadata.partition(),metadata.offset());
						System.out.println(data);
					}else {
						String data = String.format("failed : %s", exception.getMessage());
						System.out.println(data);
					}
				}
			});

		}
		prdc.close();
	}
}
