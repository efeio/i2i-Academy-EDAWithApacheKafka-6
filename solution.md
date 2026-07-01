# Section 4. Solution

This document contains the complete answers, explanations, and structure for the **Section 4. Solution** of the homework report. You can copy and paste the text below directly into your `EDAWithApacheKafka.docx` file and insert the screenshots in the marked placeholders.

---

## 4.1. Theoretical Knowledge (Section 3.1.1)

*   **What is Event-Driven Architecture (EDA), and why do we need it?**
    
    Event-Driven Architecture (EDA) is a software design pattern where decoupled services communicate asynchronously by publishing and consuming events (state changes) rather than using direct, synchronous request-response calls. We need EDA to build highly scalable, loose-coupled, and fault-tolerant systems. By decoupling producer and consumer services, if one service experiences downtime or high latency, the rest of the system remains unaffected, dramatically increasing resilience.

*   **Why has Apache Kafka become an industry standard, and what does it do better than its alternatives (e.g., RabbitMQ, ActiveMQ)?**
    
    Apache Kafka has become the industry standard because of its distributed append-only commit log architecture, which stores messages durably on disk rather than deleting them immediately after consumption. Unlike traditional brokers like RabbitMQ or ActiveMQ that rely on queue-based message delivery, Kafka enables high-throughput, low-latency streaming, and supports event replaying. This allows multiple consumer groups to read the same logs independently at their own pace, making it perfect for real-time event streaming and analytical data pipelines.

---

## 4.2. Local Environment Setup & Deployment (Section 3.1.2)

### Description
For the local setup, I ran a containerized single-node Apache Kafka broker coordinated by Zookeeper using Docker Compose. I then developed two Java applications utilizing the official Apache Kafka Client library:
1.  **User Model:** A custom Java class representing a user with `name`, `age`, and `email` properties.
2.  **BasicProducer:** Creates instances of the custom `User` object, serializes them into JSON strings using the Jackson library, and sends them to the `i2i-academy-topic` topic on the local broker.
3.  **BasicConsumer:** Subscribes to the same topic, reads the incoming records, deserializes the JSON payloads back into `User` objects, and prints the results to the console.

### Screenshots

*   **Docker Container Status (Local):**
    *(Insert your **Screenshot 1** showing Zookeeper and Kafka running in Docker Desktop or terminal here.)*
    
*   **Producer and Consumer Console Output (Local):**
    *(Insert your **Screenshot 2** showing the Producer sending messages and the Consumer printing deserialized records locally here.)*

---

## 4.3. Cloud Deployment (Section 3.1.3)

### Description
For the cloud deployment, I replicated the Docker container setup on a Google Cloud Platform (GCP) VM instance. Due to the resource-constrained nature of the VM (1GB RAM), I optimized the JVM Heap sizes for both Zookeeper (`-Xmx64m`) and Kafka (`-Xmx192m`). Furthermore, I disabled the memory-heavy Kafka Log Cleaner threads (`KAFKA_LOG_CLEANER_ENABLE: "false"`) to prevent JVM heap space Out-Of-Memory errors during boot. To enable network communication inside containerized Java environments, I configured the broker network listener (`KAFKA_LISTENERS`) to bind to `0.0.0.0:9092`, allowing Docker port forwarding to function correctly on the Linux VM. The applications were compiled and run successfully inside transient Maven Docker containers using the host network (`--network host`) to communicate with the broker.

### Screenshots

*   **Producer and Consumer Communication (Cloud VM via SSH):**
    *(Insert your **Screenshot 3** showing the upper SSH terminal running the Producer and the lower SSH terminal showing the Consumer successfully printing received records on the VM here.)*

---

## 4.4. GitHub Repository Link

The complete source code, configuration files, and git history for this homework are available at:
**GitHub Repository:** [https://github.com/efeio/i2i-Academy-EDAWithApacheKafka-6](https://github.com/efeio/i2i-Academy-EDAWithApacheKafka-6)
