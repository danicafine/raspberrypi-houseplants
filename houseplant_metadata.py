import time
import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import avro_helper


# set up configs
conf = avro_helper.read_ccloud_config("./librdkafka.config")
schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['basic.auth.user.info']
}

# set up schema registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

houseplant_avro_serializer = AvroSerializer(
        schema_registry_client = schema_registry_client,
        schema_str = avro_helper.houseplant_schema,
        to_dict = avro_helper.Houseplant.houseplant_to_dict
)

# set up Kafka producer
producer_conf = avro_helper.pop_schema_registry_params_from_config(conf)
producer_conf['value.serializer'] = houseplant_avro_serializer
producer = SerializingProducer(producer_conf)

topic = 'houseplant_metadata'

# existing plants
plants = {
    '5': {
        "plant_id": 5,
        "scientific_name": "Rhaphidophora tetrasperma",
        "common_name": "Mini Monstera",
        "given_name": "Ginny",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 40,
        "moisture_high": 70
    },
    '4': {
        "plant_id": 4,
        "scientific_name": "Pilea peperomioides",
        "common_name": "Missionary Plant",
        "given_name": "Piper",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 45,
        "moisture_high": 75
    },
    '3': {
        "plant_id": 3,
        "scientific_name": "Monstera adansonii",
        "common_name": "Swiss Cheese Plant",
        "given_name": "Bradley",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 20,
        "moisture_high": 70
    },
    '2': {
        "plant_id": 2,
        "scientific_name": "Zamioculcas zamiifolia",
        "common_name": "ZZ Plant",
        "given_name": "Francesca",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 20,
        "moisture_high": 60
    },
    '1': {
        "plant_id": 1,
        "scientific_name": "Epipremnum aureum",
        "common_name": "Golden Pothos",
        "given_name": "Nicholas",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 20,
        "moisture_high": 60
    },
    '0': {
        "plant_id": 0,
        "scientific_name": "Schefflera arboricola",
        "common_name": "Dwarf Umbrella Tree",
        "given_name": "Cyril",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 30,
        "moisture_high": 60
    }
}

for k,v in plants.items():
    # send data to Kafka
    houseplant = avro_helper.Houseplant(
            v["plant_id"], 
            v["scientific_name"],
            v["common_name"],
            v["given_name"],
            v["temperature_low"],
            v["temperature_high"],
            v["moisture_low"],
            v["moisture_high"]
        )
    producer.produce(topic, key=k, value=houseplant) 
    producer.poll()