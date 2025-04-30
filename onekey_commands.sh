PROJECT_NAME='swe5003'
PROJECT_ROOT="$(pwd)" # Automatically set the project root directory
EXPORT_TO_BIGQUERY_PIPELINE_UUID='94ab2c7a2aa24bde8e148ef84c88a10f'

# General function to run docker-compose commands
do-co() {
    local compose_file=$1
    local action=$2
    local env_file="${compose_file%/*}/.env" # Automatically detect .env file in the same directory as the compose file

    if [ -f "$env_file" ]; then
        docker-compose -f "$compose_file" --env-file "$env_file" $action
    else
        docker-compose -f "$compose_file" $action
    fi
}

# Function to log messages
log() {
    local level=$1
    local message=$2
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $message"
}

# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    log "INFO" "Network ${PROJECT_NAME}-network already exists."
fi

# Function to start streaming data
stream-data() {
    log "INFO" "Starting streaming data..."
    do-co "${PROJECT_ROOT}/infra/docker/data_stream/docker-compose.yaml" "up"
    log "INFO" "Streaming data started successfully."
}

# Function to start Kafka
start-kafka() {
    log "INFO" "Starting Kafka..."
    do-co "${PROJECT_ROOT}/infra/docker/kafka/docker-compose.yml" "up -d"
    log "INFO" "Kafka started successfully."
}

# Function to stop Kafka
stop-kafka() {
    log "INFO" "Stopping Kafka..."
    do-co "${PROJECT_ROOT}/infra/docker/kafka/docker-compose.yml" "down"
    log "INFO" "Kafka stopped successfully."
}

# Function to start Spark
start-spark() {
    log "INFO" "Starting Spark..."
    chmod +x "${PROJECT_ROOT}/scripts/build_spark.sh"
    "${PROJECT_ROOT}/scripts/build_spark.sh"
    do-co "${PROJECT_ROOT}/infra/docker/spark/docker-compose.yml" "up -d"
    log "INFO" "Spark started successfully."
}

# Function to stop Spark
stop-spark() {
    log "INFO" "Stopping Spark..."
    do-co "${PROJECT_ROOT}/infra/docker/spark/docker-compose.yml" "down"
    log "INFO" "Spark stopped successfully."
}

# Function to start Mage
start-mage() {
    log "INFO" "Starting Mage..."
    do-co "${PROJECT_ROOT}/infra/docker/mage/docker-compose.yml" "up -d"
    sleep 5
    sudo cp "${PROJECT_ROOT}/src/streaming/kafka_to_gcs_streaming/kafka_to_gcs.yaml" "${PROJECT_ROOT}/infra/docker/mage/${PROJECT_NAME}/data_exporters/"
    sudo cp "${PROJECT_ROOT}/src/streaming/kafka_to_gcs_streaming/consume_from_kafka.yaml" "${PROJECT_ROOT}/infra/docker/mage/${PROJECT_NAME}/data_loaders/"
    sudo mkdir -p "${PROJECT_ROOT}/infra/docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming"
    sudo cp "${PROJECT_ROOT}/src/streaming/kafka_to_gcs_streaming/metadata.yaml" "${PROJECT_ROOT}/infra/docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/"
    sudo touch "${PROJECT_ROOT}/infra/docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/__init__.py"
    log "INFO" "Mage started successfully."
}

# Function to stop Mage
stop-mage() {
    log "INFO" "Stopping Mage..."
    do-co "${PROJECT_ROOT}/infra/docker/mage/docker-compose.yml" "down"
    log "INFO" "Mage stopped successfully."
}

# Function to start the streaming pipeline
start-streaming-pipeline() {
    log "INFO" "Starting streaming pipeline..."
    start-kafka &
    start-mage &
    wait
    stream-data
    log "INFO" "Streaming pipeline started successfully."
}

# Function to stop the streaming pipeline
stop-streaming-pipeline() {
    log "INFO" "Stopping streaming pipeline..."
    stop-kafka &
    stop-mage &
    wait
    log "INFO" "Streaming pipeline stopped successfully."
}

# Function to execute OLAP transformation pipeline
olap-transformation-pipeline() {
    log "INFO" "Executing OLAP transformation pipeline..."
    python "${PROJECT_ROOT}/src/batch/export_to_gcs/pipeline.py"
    log "INFO" "OLAP transformation pipeline executed successfully."
}

# Function to execute GCS to BigQuery pipeline
gcs-to-bigquery-pipeline() {
    log "INFO" "Executing GCS to BigQuery pipeline..."
    curl -X POST http://localhost:6789/api/pipeline_schedules/1/pipeline_runs/00e78f98293945ec9af120891c006992 \
        --header 'Content-Type: application/json' \
        --data '
        {
            "pipeline_run": {
                "variables": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
        }'
    log "INFO" "GCS to BigQuery pipeline executed successfully."
}

# Function to start the batch pipeline
start-batch-pipeline() {
    log "INFO" "Starting batch pipeline..."
    olap-transformation-pipeline
    gcs-to-bigquery-pipeline
    log "INFO" "Batch pipeline started successfully."
}

# Git operations
gitting() {
    log "INFO" "Committing changes to Git..."
    git add .
    sleep 2
    git commit -m "Update from Local"
    sleep 2
    git push -u origin main
    log "INFO" "Changes pushed to Git successfully."
}

# Terraform operations
terraform-start() {
    log "INFO" "Initializing Terraform..."
    terraform -chdir="${PROJECT_ROOT}/infra/terraform" init
    terraform -chdir="${PROJECT_ROOT}/infra/terraform" plan
    terraform -chdir="${PROJECT_ROOT}/infra/terraform" apply
    log "INFO" "Terraform resources created successfully."
}

terraform-destroy() {
    log "INFO" "Destroying Terraform resources..."
    terraform -chdir="${PROJECT_ROOT}/infra/terraform" destroy
    log "INFO" "Terraform resources destroyed successfully."
}

# Function to start the entire project
start-project() {
    log "INFO" "Starting the entire project..."
    terraform-start
    start-streaming-pipeline
    log "INFO" "Waiting 2 minutes to gather data..."
    sleep 120
    start-spark
    start-batch-pipeline
    dbt run
    start-metabase
    log "INFO" "Project started successfully."
}

# Function to stop all services
stop-all-services() {
    log "INFO" "Stopping all services..."
    stop-mage &
    stop-kafka &
    stop-spark &
    stop-metabase &
    wait
    log "INFO" "All services stopped successfully."
}