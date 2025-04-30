# Function to stop containers
stop_containers() {
    echo "Closing $1 related containers"
    docker-compose -f "$2" down
    echo
}

# Stop containers
stop_containers "Mage" "./docker/mage/docker-compose.yml"
stop_containers "Kafka" "./docker/kafka/docker-compose.yml"
stop_containers "Spark" "./docker/spark/docker-compose.yml"

echo "Closed all containers."