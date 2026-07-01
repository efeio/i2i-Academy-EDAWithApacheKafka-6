package com.i2iacademy.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.i2iacademy.kafka.model.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BasicConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "i2i-academy-topic";
    private static final String GROUP_ID = "i2i-academy-group";

    public static void main(String[] args) {
        // 1. Configure Consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Instantiate KafkaConsumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            ObjectMapper objectMapper = new ObjectMapper();

            // 3. Subscribe to the Topic
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("Consumer is running and listening to topic: " + TOPIC);
            System.out.println("Press Ctrl+C to stop.");

            // 4. Poll Loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Deserialize JSON string back to Custom User object
                        User user = objectMapper.readValue(record.value(), User.class);
                        
                        System.out.println("\n------------------------------------------------");
                        System.out.println("Received Record Details:");
                        System.out.println("Key: " + record.key());
                        System.out.println("Partition: " + record.partition());
                        System.out.println("Offset: " + record.offset());
                        System.out.println("Deserialized Custom User Object: " + user.toString());
                        System.out.println("------------------------------------------------");
                    } catch (Exception parseException) {
                        System.err.println("Failed to deserialize value: " + record.value());
                        parseException.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Exception occurred in Consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
