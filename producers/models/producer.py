"""Producer base-class providing common utilites and functionality"""
import time
import logging

# Kafka confluent
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic

# Project modules
from settings import BROKER_URL, SCHEMA_REGISTRY_URL


# Module configurations
logger = logging.getLogger(__name__)


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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL
        }

        # Schema registry
        schema_registry = avro.CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties, schema_registry=schema_registry)

        # Kafka Admin
        self.admin = AdminClient(self.broker_properties)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

    def __del__(self):
        self.close()

    def topic_exists(self) -> bool:
        """
        Method check given topic exist or not
        """
        return self.admin.list_topics().topics.get(self.topic_name)

    def create_topic(self) -> None:
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #

        # Check exist or not
        if self.topic_exists():
            logger.info(f"Topic {self.topic_name} exist already")
            return

        print(f"Create topic {self.topic_name}")

        futures = self.admin.create_topics([
            NewTopic(
                self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )
        ], validate_only=True)

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Created topic {topic}")
            except Exception as e:
                logger.error(f"Error when create topic {topic}: {e}")
                raise e

    @staticmethod
    def time_millis() -> int:
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #

        # delete topics
        if self.topic_exists():
            pass
            # futures = self.admin.deleteTopics([self.topic_name])
            # for topic, futures in futures.items():
            #     futures.result()
            #     logger.info(f"Delete topic {self.topic_name}")

        logger.info("producer close incomplete - skipping")
