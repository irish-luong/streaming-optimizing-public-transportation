"""Defines core consumer functionality"""
import logging
import confluent_kafka
from tornado import gen
from abc import abstractmethod
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from settings import BROKER_URL, SCHEMA_REGISTRY_URL, GROUP_ID

logger = logging.getLogger(__name__)


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
            "group.id": GROUP_ID,
            "max.poll.interval.ms": 60000,
            "bootstrap.servers": BROKER_URL,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest" if offset_earliest else "latest"
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            logger.info(f"Look partition {partition} of topic {self.topic_name_pattern}")

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        message = self.consumer.poll(self.consume_timeout)

        if message is None:
            logger.debug("Consumed a None message")
            return 0

        if message.error():
            logger.error("Got error when consuming message")
            return 1
        else:
            self.message_handler(message)
            return 1

        return 0

    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("Close kafka consumer")
        self.consumer.close()