package com.learning.kafka;

import com.learning.kafka.customuserdata.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        properties.put("value.serializer", "com.learning.kafka.customuserdata.UserSerializer");

        // create producer
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

        //send data
        String topic = "first-topic";
        String key = "test_key";
        User msg = new User();

        ProducerRecord<String, User> record = new ProducerRecord<>(topic, key, msg);
        producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                    if (null == e) {
                        System.out.println("Partition : " + recordMetadata.partition());
                        System.out.println("Topic : " + recordMetadata.topic());
                        System.out.println("Offset : " + recordMetadata.offset());
                        System.out.println("Timestamp : " + LocalDateTime.ofInstant(Instant.ofEpochMilli(recordMetadata.timestamp()), ZoneId.systemDefault()));
                    } else {
                        System.out.println("Error occurred " + e);
                    }

                }
        );

        System.out.println("Message " + msg + " sent!!");

        // flush producer
        producer.flush();

        //  close producer
        producer.close();
    }
}
