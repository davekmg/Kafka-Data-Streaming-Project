"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


# Unlike previous versions of KSQLDB, this version requires tables to have a PRIMARY KEY column, and does not implicitly
# create one if it is not defined. 
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    ROWKEY VARCHAR PRIMARY KEY,
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',
        VALUE_FORMAT='AVRO'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON', KEY_FORMAT='JSON') AS
    SELECT station_id, COUNT(station_id) as count
    FROM turnstile
    GROUP BY station_id;

"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
       return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        # Ensure that a 2XX status code was returned
        resp.raise_for_status()

        print("Executed successfully")
    except:
        print("Sorry an error occured")


if __name__ == "__main__":
    execute_statement()
