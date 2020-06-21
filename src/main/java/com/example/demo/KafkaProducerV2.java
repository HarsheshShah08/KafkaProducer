package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class KafkaProducerV2 {

	public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        // TODO: Produce records with keys and check if they are assigned to partitions as expected.
        ProducerRecord<Integer, String> r1 =
                new ProducerRecord<>("MySecondTopicForDemo", 1000, "Record created on 1001 with r1");
        var r2 = new ProducerRecord<>("MySecondTopicForDemo", 1001, "Record created on 1002 with r2");
        var r3 = new ProducerRecord<>("MySecondTopicForDemo", 1000, "Record created on 1001 with r3");

        RecordMetadata rm = producer.send(r1).get();
        System.out.println();
        System.out.printf("Value with key %d assigned to partition: %d%n", r1.key(), rm.partition());
        rm = producer.send(r2).get();
        System.out.printf("Value with key %d assigned to partition: %d%n", r2.key(), rm.partition());
        rm = producer.send(r3).get();
        System.out.printf("Value with key %d assigned to partition: %d%n", r3.key(), rm.partition());
        System.out.println();

        producer.flush();
        producer.close();
    }


}


