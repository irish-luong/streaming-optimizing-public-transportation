"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check

from settings import KSQL_URL

logger = logging.getLogger(__name__)

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statement, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id BIGINT,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
) WITH (
    KAFKA_TOPIC='turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT='JSON'
) AS
SELECT
    station_id,
    SUM(num_entries) as count
FROM
    turnstile
GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print("TURNSTILE_SUMMARY exist")
        return

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {
                    "ksql.streams.auto.offset.reset": "earliest"
                },
            }
        ),
    )


    print(resp.json())
    resp.raise_for_status()


if __name__ == "__main__":
    # print("xxx")
    execute_statement()
