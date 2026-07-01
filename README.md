# i2i-Academy-EDAWithApacheKafka-6

This project is developed as part of the i2i Academy to understand the principles of Event-Driven Architecture (EDA) and gain hands-on experience with Apache Kafka by setting up a containerized broker and building Java-based Producer and Consumer applications.

## 3.1.1. Theoretical Knowledge

*   **What is Event-Driven Architecture (EDA), and why do we need it?**
    To put it simply, Event-Driven Architecture (EDA) is where systems communicate by publishing 'events' (like a user registering) instead of calling each other directly and waiting for a reply. This decoupling is a lifesaver because if one service crashes, the rest of the application keeps running smoothly, making the whole system much more resilient.

*   **Why has Apache Kafka become an industry standard, and what does it do better than its alternatives (e.g., RabbitMQ, ActiveMQ)?**
    Kafka became the industry standard because it is built like a massive, super-fast log that writes all messages to disk rather than deleting them right after they are read. This allows different services to consume the same data at their own pace or even replay old messages, which is something traditional brokers like RabbitMQ just can't do easily.
