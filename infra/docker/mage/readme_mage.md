# MercaForesight - Mage Configuration

## **Directory Overview**
This folder contains configuration files related to the Mage data pipeline tool. Mage is used to build, orchestrate, and monitor data pipelines, enabling efficient data processing and integration within the MercaForesight platform.

---

## **Purpose**
- Set up and manage Mage for building and orchestrating data pipelines.
- Enable seamless integration of data from multiple sources.
- Monitor and optimize data workflows for better performance and reliability.

---

## **Current Files**
- **`docker-compose.yml`**: Defines the Docker configuration for running Mage and its dependencies.
- **`.env`**: Contains environment variables for configuring Mage (e.g., database connections, pipeline settings).

---

## **Usage Guidelines**
1. Start the Mage service using the following command:
   ```bash
   docker-compose up -d
   ```
2. Access the Mage UI in your browser at http://localhost:6789 (default port).
3. To stop and remove the Mage container, use:
   ```bash
   docker-compose down
   ```
4. Update the `.env` file to customize Mage configurations, such as database connections or pipeline parameters.

---

## **Notes**
- Ensure that the `.env` file is properly configured before starting the service.
- Regularly back up pipeline configurations and metadata to avoid data loss.
- Monitor logs for troubleshooting:
   ```bash
   docker logs <container_name>
   ```
- Use Mage's built-in monitoring tools to track pipeline performance and identify bottlenecks.

---