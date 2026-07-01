package com.i2iacademy.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.i2iacademy.kafka.model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class BasicProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "i2i-academy-topic";

    public static void main(String[] args) {
        // 1. Configure Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Instantiate KafkaProducer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ObjectMapper objectMapper = new ObjectMapper();

            // 3. Create dummy Custom User objects
            User[] users = {
                new User("Alice Smith", 28, "alice.smith@example.com"),
                new User("Bob Jones", 34, "bob.jones@example.com"),
                new User("Charlie Brown", 22, "charlie.brown@example.com")
            };

            System.out.println("Starting to send custom User objects to topic: " + TOPIC);

            for (int i = 0; i < users.length; i++) {
                User user = users[i];
                // Serialize Java object to JSON String
                String jsonValue = objectMapper.writeValueAsString(user);

                // Create a record
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key-" + i, jsonValue);

                // Send data
                Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Successfully sent message!");
                        System.out.println("Topic: " + metadata.topic());
                        System.out.println("Partition: " + metadata.partition());
                        System.out.println("Offset: " + metadata.offset());
                    } else {
                        System.err.println("Error while producing message: " + exception.getMessage());
                    }
                });

                // Wait for the message to be acknowledged (synchronous send for demonstration)
                future.get();
                
                // Sleep brief moment between sends
                Thread.sleep(1000);
            }

            System.out.println("All messages sent successfully.");
        } catch (Exception e) {
            System.err.println("Exception occurred in Producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
