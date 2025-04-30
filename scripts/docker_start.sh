PROJECT_ROOT="$(pwd)"

# Create Docker network and volume
docker network create "${PROJECT_NAME}-network"
docker volume create --name=hadoop-distributed-file-system

# Start containers
start_containers() {
    echo "Starting $1 related containers"
    docker-compose -f "$2" up -d
    echo
}

start_containers "Mage" "${PROJECT_ROOT}/infra/docker/mage/docker-compose.yml"
start_containers "Kafka" "${PROJECT_ROOT}/infra/docker/kafka/docker-compose.yml"

echo "Starting Spark related containers"
chmod +x "${PROJECT_ROOT}/scripts/build_spark.sh"
./docker/spark/build.sh
echo
docker-compose -f ./docker/spark/docker-compose.yml up -d
echo

echo "Started all containers. You can check their status with 'docker ps'."