package br.com.viavarejo.kafka.service.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	
	private static final String TOPIC = "teste";
	private static final String BOOTSTRAP_SERVER = "172.18.240.47:9092";

	public Producer() {
	}

	public void build(String message) {
		System.out.println("Init");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC, message);
		producer.send(producerRecord);
		producer.flush();
		producer.close();
		System.out.println("Finish");
	}

	private Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public static void main(String[] args) {
		try {
			String message = "Hello world";
			if(args.length == 1) {
				message = args[0];
			}
			new Producer().build(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Fim main");
	}

}
