# MercaForesight - Kafka Configuration

## **Directory Overview**
This folder contains configuration files related to the Kafka service, which is used as a message broker to enable communication between different services or components in the MercaForesight platform.

---

## **Purpose**
- Set up and manage Kafka as a message broker for the project.
- Facilitate real-time communication and data streaming between services.
- Ensure reliable message delivery and processing.

---

## **Current Files**
- **`docker-compose.yml`**: Defines the Docker configuration for running Kafka and its dependencies (e.g., Zookeeper).
- **`.env`**: Contains environment variables for configuring Kafka (e.g., broker ID, log directories, advertised listeners).

---

## **Usage Guidelines**
1. Use the following command to start the Kafka service:
   ```bash
   docker-compose up -d
   ```
2. Verify that Kafka is running by connecting to the broker using a Kafka client or CLI.
3. To stop and remove the Kafka container, use:
   ```bash
   docker-compose down
   ```
4. Update the `.env` file to customize Kafka configurations (e.g., port, log retention).

---

## **Notes**
- Ensure that the `.env` file is properly configured before starting the service.
- Avoid exposing Kafka ports to the public internet without proper security measures (e.g., firewalls, authentication).
- Monitor Kafka logs for troubleshooting:
   ```bash
   docker logs <container_name>
   ```
- Use tools like Kafka Manager or Prometheus for monitoring Kafka performance and health.

---