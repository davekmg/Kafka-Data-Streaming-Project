"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


# Note: The table does not pull any data when the primary key is of type int. Therfore,
# the station_id has used implicit type coercion by setting the type as VARCHAR
# Note: The GROUP BY column does not appear in the topic VALUE but is set in the topic KEY. 
# Note: The table turnstile_summary was created with the stream turnstile_stream instead of the table turnstile
# becasue the current version of ksql, as per this writing, requires a primary key definition when creating a table. This primary key
# is encoded in the table making it unreadable. The stream was used an alternative option because KEY definition is not mandatory.
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id VARCHAR PRIMARY KEY,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',
        VALUE_FORMAT='AVRO'
);

CREATE STREAM turnstile_stream (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',
        VALUE_FORMAT='AVRO'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON', KEY_FORMAT='JSON') AS
    SELECT station_id, COUNT(station_id) as count
    FROM turnstile_stream
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
