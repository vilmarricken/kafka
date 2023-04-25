package br.com.viavarejo.kafka.service.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

	private static final String TOPIC = "teste";
	private static final String BOOTSTRAP_SERVER = "172.18.240.47:9092";

	private KafkaConsumer<String, String> connect() {
		return new KafkaConsumer<String, String>(properties());
	}

	public void list() {
		KafkaConsumer<String, String> consumer = connect();
		Map<String, List<PartitionInfo>> topics = consumer.listTopics();
		Set<Entry<String, List<PartitionInfo>>> set = topics.entrySet();
		for (Entry<String, List<PartitionInfo>> entry : set) {
			System.out.println(entry.getKey());
			List<PartitionInfo> partitions = entry.getValue();
			for (PartitionInfo partition : partitions) {
				System.out.println("\t" + partition);
			}
		}
	}

	private void consumer() {
		KafkaConsumer<String, String> consumer = connect();
		consumer.subscribe(Collections.singletonList(TOPIC));
		System.out.println("Waiting records");
		for (int i = 0; i < 120; i++) {
			final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			System.out.println("Count: " + consumerRecords.count());
			if (consumerRecords.count() > 0) {
				System.out.println("Partitions: " + consumerRecords.partitions());
				Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
				while (iterator.hasNext()) {
					System.out.println(iterator.next());
				}
				consumer.commitAsync();
			}
		}
		consumer.unsubscribe();
	}

	private Properties properties() {

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer2");
		return properties;
	}

	public static void main(String[] args) {
		int option = 0;
		if (args.length == 1) {
			try {
				option = Integer.parseInt(args[0]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Init");
		try {
			Consumer consumer = new Consumer();
			switch (option) {
			case 0:
			default:
				consumer.consumer();
				break;
			case 1:
				consumer.list();
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Finish");
	}

}
