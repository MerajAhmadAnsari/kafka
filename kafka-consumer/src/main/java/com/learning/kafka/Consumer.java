package com.learning.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Hello world!
 */
public class Consumer {
    public static void main(String[] args) {

        //Create properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", "my-test-group");
        properties.put("auto.offset.reset", "earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe to topic
        consumer.subscribe(Arrays.asList("first-topic"));

        //Poll for data
        while (true) {
            System.out.println("Waiting for new messages...");

            ConsumerRecords<String, String> records = consumer.poll(1000);
            for( ConsumerRecord<String, String> singleRecord : records) {
                System.out.println("Key : " + singleRecord.key());
                System.out.println("Value : " + singleRecord.value());
                System.out.println("Offset : " + singleRecord.offset());
                System.out.println("Partition : " + singleRecord.partition());
            }
        }
    }
}
