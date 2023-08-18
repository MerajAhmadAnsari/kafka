package com.learning.kafka;

import com.learning.kafka.customuserdata.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
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
        properties.put("value.deserializer", "com.learning.kafka.customuserdata.UserDeSerializer");
        properties.put("group.id", "my-test-group");
        properties.put("auto.offset.reset", "earliest");

        //Create Consumer
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);

        // handle graceful exit
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("Handling shutdown, waking up consumer...");
                    consumer.wakeup();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
        );
        //Subscribe to topic
        consumer.subscribe(Arrays.asList("first-topic"));

        //Poll for data
        try {
            while (true) {
                System.out.println("Waiting for new messages...");

                ConsumerRecords<String, User> records = consumer.poll(1000);
                for (ConsumerRecord<String, User> singleRecord : records) {
                    System.out.println("Key : " + singleRecord.key());
                    System.out.println("Value : " + singleRecord.value());
                    System.out.println("Offset : " + singleRecord.offset());
                    System.out.println("Partition : " + singleRecord.partition());
                }
            }
        } catch (WakeupException e) {
            System.out.println("Consumer is going to shutdown");
        } catch (Exception e) {
            System.out.println("Generic error : " + e.getMessage());
        } finally {
            consumer.close();
        }

    }
}
