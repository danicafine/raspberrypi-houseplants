from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.error import SerializationError
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from classes.reading import Reading
from classes.houseplant import Houseplant
from classes.mapping import Mapping

import yaml


def config():
	# fetches the configs from the available file
	with open('./config/config.yaml', 'r') as config_file:
		config = yaml.load(config_file, Loader=yaml.CLoader)

		return config


def sr_client():
	# set up schema registry
	sr_conf = config()['schema-registry']
	sr_client = SchemaRegistryClient(sr_conf)

	return sr_client


def reading_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Reading.get_schema(),
		from_dict = Reading.dict_to_reading
		)


def houseplant_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Houseplant.get_schema(),
		from_dict = Houseplant.dict_to_houseplant
		)


def mapping_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Mapping.get_schema(),
		from_dict = Mapping.dict_to_mapping
		)


def reading_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = Reading.get_schema(),
		to_dict = Reading.reading_to_dict
		)


def houseplant_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = Houseplant.get_schema(),
		to_dict = Houseplant.houseplant_to_dict
		)


def producer(value_serializer):
	producer_conf = config()['kafka'] | { 'value.serializer': value_serializer }
	return SerializingProducer(producer_conf)


def consumer(value_deserializer, group_id, topics):
	consumer_conf = config()['kafka'] | {'value.deserializer': value_deserializer,
										  'group.id': group_id,
										  'auto.offset.reset': 'earliest',
										  'enable.auto.commit': 'false'
										  }

	consumer = DeserializingConsumer(consumer_conf)
	consumer.subscribe(topics)

	return consumer