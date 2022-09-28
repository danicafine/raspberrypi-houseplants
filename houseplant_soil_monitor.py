import time

#from board import SCL, SDA
#import busio
#from adafruit_seesaw.seesaw import Seesaw

from classes.reading import Reading 

from helpers import clients,logging

logger = logging.set_logging('houseplant_soil_monitor')
config = clients.config()

def produce_sensor_readings(producer, plant_addresses):
    # i2c_bus = busio.I2C(SCL, SDA)
    for plant_id,address in plant_addresses.items():
        try:
            # ss = Seesaw(i2c_bus, addr=int(address, 16))
            sensor_values = config['raspberry-pi']

            # read moisture 
            touch = 0.0 #ss.moisture_read()
            if touch < sensor_values['sensor-low']:
                touch = sensor_values['sensor-low']
            elif touch > sensor_values['sensor-high']:
                touch = sensor_values['sensor-high']

            touch_percent = (touch - sensor_values['sensor-low']) / (sensor_values['sensor-high'] - sensor_values['sensor-low']) * 100

            # read temperature
            temp = 0.0 #ss.get_temp()
        
            # send data to Kafka
            reading = Reading(int(plant_id), round(touch_percent, 3), round(temp, 3))

            logger.info(f"Publishing message: key, value: ({plant_id},{reading})")
            producer.produce(config['topics']['test'], key=plant_id, value=reading) 
        except Exception as e:
            logger.error("Got exception %s", e)
        finally:
            producer.poll()
            producer.flush()


if __name__ == '__main__':
    # set up Kafka Producer for Readings
    producer = clients.producer(clients.reading_serializer())

    # set up mapping between id and address
    plant_addresses = {
    '0': 0x39,
    '3': 0x37,
    '4': 0x36,
    '5': 0x38
    }

    # start readings capture loop
    try:
        while True:
            # later, update mappings

            # capture readings from sensors
            produce_sensor_readings(producer, plant_addresses)

            time.sleep(30)
    finally:
        producer.flush()
