"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            #"compression.type": "lz4",
            #"linger.ms":"10000",
            #"batch.num.messages": "",
            #"queue.buffering.max.messages": "",
            #"queue.buffering.max.kbytes": ""
        }

        # create a CachedSchemaRegistryClient. 
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # create the topic if it does not yet exist
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # instantiate the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties, 
            schema_registry=schema_registry
        )

    def create_topic(self):
        """
        Creates the producer topic if it does not already exist
        """

        client = AdminClient({"bootstrap.servers": BROKER_URL})
        futures = client.create_topics(
            [NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                 print(f"failed to create topic {self.topic_name}: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))
    
    def gen_topic_name(self, type_name, name):
        """
        Generates topic name
        """
        
        station_name = (
            name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        if type_name == "station":
            topic_name = f"org.chicago.cta.station.arrivals.{station_name}"
        elif type_name == "turnstile":
            #topic_name = f"org.chicago.cta.station.turnstile.{station_name}"
            # Note: Using one topic for turnstiles because KSQL can only read from one topic for a stream/table
            topic_name = f"org.chicago.cta.turnstiles"
        else:
            raise Exception(f"Sorry, type name '{type_name}' not supported ")

        return topic_name

    def close(self):
        """
        Prepares the producer for exit by cleaning up the producer
        """

        print("Flushing records...")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
