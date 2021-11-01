"""Contains functionality related to Weather"""
import json
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 10.0
        self.status = "cloudy"

    def process_message(self, message):
        """Handles incoming weather data"""
        value = message.value()
        self.temperature = value["temperature"]
        self.status = value["status"]



