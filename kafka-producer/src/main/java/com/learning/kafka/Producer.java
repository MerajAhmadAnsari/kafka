package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * Hello world!
 */
public class Producer {
    public static void main(String[] args) {
        // producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //send data
        String msg = "SDK : " + new Random().nextInt();
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", msg);
        producer.send(record);

        System.out.println("Message " + msg + " sent!!");

        // flush producer
        producer.flush();

        //  close producer
        producer.close();
    }
}
