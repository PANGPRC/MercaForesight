# MercaForesight - Spark Configuration

## **Directory Overview**
This folder contains configuration files related to the Apache Spark distributed computing framework. Spark is used for large-scale data processing, enabling efficient analytics and machine learning workflows within the MercaForesight platform.

---

## **Purpose**
- Set up and manage Spark for distributed data processing.
- Enable scalable data analytics and machine learning workflows.
- Integrate Spark with other components of the MercaForesight platform.

---

## **Current Files**
- **`docker-compose.yml`**: Defines the Docker configuration for running Spark and its dependencies (e.g., Spark Master, Spark Workers).
- **`.env`**: Contains environment variables for configuring Spark (e.g., memory allocation, worker settings).

---

## **Usage Guidelines**
1. Start the Spark cluster using the following command:
   ```bash
   docker-compose up -d
   ```
2. Access the Spark Web UI in your browser at http://localhost:8080 (default port for Spark Master).
3. To stop and remove the Spark containers, use:
   ```bash
   docker-compose down
   ```
4. Update the `.env` file to customize Spark configurations, such as memory limits or the number of workers.

---

## **Notes**
- Ensure that the `.env` file is properly configured before starting the Spark cluster.
- Monitor Spark logs for troubleshooting:
   ```bash
   docker logs <container_name>
   ```
- Use Spark's Web UI to monitor job execution and cluster performance.
- Regularly update Spark configurations to align with project requirements and optimize performance.

---