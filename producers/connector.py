"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations-1"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
    KAFKA_CONNECT_URL,
    headers={"Content-Type": "application/json"},
    data=json.dumps(
            {
                "name": CONNECTOR_NAME,  
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # TODO
                    "topic.prefix": "org.chicago.cta.tables.",  
                    "mode": "incrementing", 
                    "incrementing.column.name": "stop_id", 
                    "table.whitelist": "stations",  
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "poll.interval.ms": "1800000", # 30min
                },
            }
        ),
    )
    try:
        # Ensure a healthy response was given
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
   
if __name__ == "__main__":
    configure_connector()
