import time
import json
import logging

from board import SCL, SDA
import busio
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from adafruit_seesaw.seesaw import Seesaw

from classes.reading import Reading 
from classes.houseplant import Houseplant

from helpers import clients

logger = logging.set_logging('houseplant_soil_monitor')
config = clients.config()

# set up schema registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

reading_avro_serializer = AvroSerializer(
        schema_registry_client = schema_registry_client,
        schema_str = avro_helper.reading_schema,
        to_dict = avro_helper.Reading.reading_to_dict
)

# set up Kafka producer
producer_conf = avro_helper.pop_schema_registry_params_from_config(conf)
producer_conf['value.serializer'] = reading_avro_serializer
producer = SerializingProducer(producer_conf)

topic = 'houseplant-readings'

# set up mapping between id and address
plant_addresses = {
    '0': 0x39,
    '3': 0x37,
    '4': 0x36,
    '5': 0x38
}

while True:
    i2c_bus = busio.I2C(SCL, SDA)
    for k,v in plant_addresses.items():
        try:
            ss = Seesaw(i2c_bus, addr=v)

            # read moisture 
            touch = ss.moisture_read()
            if touch < TOUCH_LO:
                touch = TOUCH_LO
            elif touch > TOUCH_HI:
                touch = TOUCH_HI

            touch_percent = (touch - TOUCH_LO) / (TOUCH_HI - TOUCH_LO) * 100

            # read temperature
            temp = ss.get_temp()
        
            # send data to Kafka
            ts = int(time.time())
            reading = avro_helper.Reading(int(k), ts, round(touch_percent, 3), round(temp, 3))

            logger.info('Publishing for key ' + str(k))
            producer.produce(topic, key=k, value=reading) 
            producer.poll()

        except Exception as e:
            print(str(e))
            logger.error('Got exception ' + str(e))

    time.sleep(5)
