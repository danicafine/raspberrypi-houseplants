import argparse, sys
from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4

houseplant_schema = """
{ 
    "name": "houseplant",
    "namespace": "com.houseplants",
    "type": "record",
    "doc": "Houseplant metadata.",
    "fields": [
        {
            "doc": "Unique plant identification number.",
            "name": "plant_id",
            "type": "int"
        },
        {
            "doc": "Scientific name of the plant.",
            "name": "scientific_name",
            "type": "string"
        },
        {
            "doc": "The common name of the plant.",
            "name": "common_name",
            "type": "string"
        },
        {
            "doc": "The given name of the plant.",
            "name": "given_name",
            "type": "string"
        },
        {
            "doc": "Lowest temperature of the plant.",
            "name": "temperature_low",
            "type": "float"
        },
        {
            "doc": "Highest temperature of the plant.",
            "name": "temperature_high",
            "type": "float"
        },
        {
            "doc": "Lowest moisture of the plant.",
            "name": "moisture_low",
            "type": "float"
        },
        {
            "doc": "Highest moisture of the plant.",
            "name": "moisture_high",
            "type": "float"
        }
    ]
}
"""

class Houseplant(object):
    """Houseplant stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id", 
        "scientific_name",
        "common_name",
        "given_name",
        "temperature_low",
        "temperature_high",
        "moisture_low",
        "moisture_high"
    ]
    
    def __init__(self, plant_id, scientific_name, common_name, given_name, 
                       temperature_low, temperature_high, moisture_low, moisture_high):
        self.plant_id         = plant_id
        self.scientific_name  = scientific_name
        self.common_name      = common_name
        self.given_name       = given_name
        self.temperature_low  = temperature_low
        self.temperature_high = temperature_high
        self.moisture_low     = moisture_low
        self.moisture_high    = moisture_high

    @staticmethod
    def dict_to_houseplant(obj, ctx):
        return Houseplant(
                obj['plant_id'],
                obj['scientific_name'],
                obj['common_name'],    
                obj['given_name'],    
                obj['temperature_low'],    
                obj['temperature_high'],    
                obj['moisture_low'],    
                obj['moisture_high'],    
            )

    @staticmethod
    def houseplant_to_dict(houseplant, ctx):
        return Houseplant.to_dict(houseplant)

    def to_dict(self):
        return dict(
                    plant_id         = self.plant_id, 
                    scientific_name  = self.scientific_name,
                    common_name      = self.common_name,
                    given_name       = self.given_name,
                    temperature_low  = self.temperature_low,
                    temperature_high = self.temperature_high,
                    moisture_low     = self.moisture_low,
                    moisture_high    = self.moisture_high
                )

reading_schema = """
{
    "name": "reading",
    "namespace": "com.houseplants",
    "type": "record",
    "doc": "Houseplant reading taken from meters.",
    "fields": [
        {
            "doc": "Unique plant indentification number.",
            "name": "plant_id",
            "type": "int"
        },
        {
            "doc": "Timestamp at which the reading was taken",
            "logicalType": "timestamp-millis",
            "name": "timestamp",
            "type": "long"
        },
        {
            "doc": "Soil moisture as a percentage.",
            "name": "moisture",
            "type": "float"
        },
        {
            "doc": "Temperature in degrees C of the soil of this plant.",
            "name": "temperature",
            "type": "float"
        }
    ]
}
"""

class Reading(object):
    """Reading stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id",
        "timestamp", 
        "moisture",
        "temperature"
    ]
    
    def __init__(self, plant_id, timestamp, moisture, temperature):
        self.plant_id    = plant_id
        self.timestamp   = timestamp
        self.moisture    = moisture
        self.temperature = temperature

    @staticmethod
    def dict_to_reading(obj, ctx):
        return reading(
                obj['plant_id'],
                obj['timestamp'],
                obj['moisture'],    
                obj['temperature'],    
            )

    @staticmethod
    def reading_to_dict(reading, ctx):
        return Reading.to_dict(reading)

    def to_dict(self):
        return dict(
                    plant_id    = self.plant_id,
                    timestamp   = self.timestamp,
                    moisture    = self.moisture,
                    temperature = self.temperature
                )



def read_ccloud_config(config_file):
    """Read Confluent Cloud configuration for librdkafka clients"""
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


def pop_schema_registry_params_from_config(conf):
    """Remove potential Schema Registry related configurations from dictionary"""
    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)
    
    return conf