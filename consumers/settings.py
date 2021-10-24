import os


GROUP_ID = os.getenv("GROUP_ID", "faust")
KSQL_URL = os.getenv("KSQL_URL")
BROKER_URL = os.getenv("BROKER_URL")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
