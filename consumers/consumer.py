"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen
import asyncio # for testing

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest


        self.broker_properties = {
            "bootstrap.servers": BROKER_URL, 
            "group.id": "0",
            "auto.offset.reset": "earliest" if self.offset_earliest else "latest"
        }

        # creates the consumer, using the appropriate type.
        if is_avro is True:
            schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            self.consumer  = AvroConsumer(
                self.broker_properties,
                schema_registry=schema_registry,
            )
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        # subscribe to kafka topic
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

            logger.info("partitions assigned for %s", self.topic_name_pattern)
            consumer.assign(partitions)
    
    # for testing
    async def consume_test(self):
        """Consumes data from the Kafka Topic"""
        c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
        c.subscribe([self.topic_name_pattern])
        #c.subscribe(['^.*org.chicago.cta.station'])
        #c.subscribe(['^.*org.chicago.cta.station.arrivals'])
        #c.subscribe(['^org.chicago.cta.station.arrivals.*'])
        while True:
            message = c.poll(1.0)
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")
            await asyncio.sleep(2.5)

    # for testing
    async def consume_test_avro(self):
        
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        c = AvroConsumer(
            {"bootstrap.servers": BROKER_URL, "group.id": "0"},
            schema_registry=schema_registry,
        )
        c.subscribe([self.topic_name_pattern])
        while True:
            message = c.poll(1.0)
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                try:
                    print(message.value())
                except KeyError as e:
                    print(f"Failed to unpack message {e}")
            await asyncio.sleep(1.0)


    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)
            #await asyncio.sleep(0.5) # for testing

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # Poll Kafka for messages.
        # Returns 1 when a message is processed, and 0 when no message is retrieved.
        message = self.consumer.poll(timeout=self.consume_timeout)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                #print(message.value())
                # call the message handler
                self.message_handler(message)

                print(f"consumed message {message.key()}: {message.value()}")
                #return 1
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        
        return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        # close down consumer to commit final offsets.
        self.consumer.close()
