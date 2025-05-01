import json
import logging
import requests
from typing import Dict, Any
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CDCManager:
    """Manages CDC (Change Data Capture) operations using Debezium"""
    
    def __init__(self, debezium_url: str = "http://localhost:8083"):
        self.debezium_url = debezium_url
        self.connectors_endpoint = f"{debezium_url}/connectors"
        
    def register_connector(self, config_file: str) -> None:
        """Register a new Debezium connector using configuration from file"""
        try:
            # Read connector configuration
            with open(config_file, 'r') as f:
                connector_config = json.load(f)
                
            # Register connector with Debezium
            response = requests.post(
                self.connectors_endpoint,
                headers={"Content-Type": "application/json"},
                data=json.dumps(connector_config)
            )
            
            if response.status_code == 201:
                logger.info(f"Successfully registered connector: {connector_config['name']}")
            else:
                logger.error(f"Failed to register connector. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Connector registration failed")
                
        except Exception as e:
            logger.error(f"Error registering connector: {e}")
            raise
            
    def delete_connector(self, connector_name: str) -> None:
        """Delete an existing Debezium connector"""
        try:
            response = requests.delete(f"{self.connectors_endpoint}/{connector_name}")
            
            if response.status_code == 204:
                logger.info(f"Successfully deleted connector: {connector_name}")
            else:
                logger.error(f"Failed to delete connector. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Connector deletion failed")
                
        except Exception as e:
            logger.error(f"Error deleting connector: {e}")
            raise
            
    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """Get the status of a Debezium connector"""
        try:
            response = requests.get(f"{self.connectors_endpoint}/{connector_name}/status")
            
            if response.status_code == 200:
                status = response.json()
                logger.info(f"Connector status for {connector_name}: {status['connector']['state']}")
                return status
            else:
                logger.error(f"Failed to get connector status. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Failed to get connector status")
                
        except Exception as e:
            logger.error(f"Error getting connector status: {e}")
            raise
            
    def pause_connector(self, connector_name: str) -> None:
        """Pause a running Debezium connector"""
        try:
            response = requests.put(f"{self.connectors_endpoint}/{connector_name}/pause")
            
            if response.status_code == 202:
                logger.info(f"Successfully paused connector: {connector_name}")
            else:
                logger.error(f"Failed to pause connector. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Connector pause failed")
                
        except Exception as e:
            logger.error(f"Error pausing connector: {e}")
            raise
            
    def resume_connector(self, connector_name: str) -> None:
        """Resume a paused Debezium connector"""
        try:
            response = requests.put(f"{self.connectors_endpoint}/{connector_name}/resume")
            
            if response.status_code == 202:
                logger.info(f"Successfully resumed connector: {connector_name}")
            else:
                logger.error(f"Failed to resume connector. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Connector resume failed")
                
        except Exception as e:
            logger.error(f"Error resuming connector: {e}")
            raise
            
    def restart_connector(self, connector_name: str) -> None:
        """Restart a Debezium connector"""
        try:
            response = requests.post(f"{self.connectors_endpoint}/{connector_name}/restart")
            
            if response.status_code == 204:
                logger.info(f"Successfully restarted connector: {connector_name}")
            else:
                logger.error(f"Failed to restart connector. Status: {response.status_code}, Response: {response.text}")
                raise Exception("Connector restart failed")
                
        except Exception as e:
            logger.error(f"Error restarting connector: {e}")
            raise

def main():
    """Example usage of CDCManager"""
    try:
        # Initialize CDC manager
        cdc_manager = CDCManager()
        
        # Register MySQL connector
        cdc_manager.register_connector('src/ingestion/cdc_config.json')
        
        # Check connector status
        status = cdc_manager.get_connector_status('mysql-connector')
        logger.info(f"Connector status: {status}")
        
    except Exception as e:
        logger.error(f"Error in CDC manager main: {e}")
        raise

if __name__ == "__main__":
    main() 