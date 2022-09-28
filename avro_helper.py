


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


class Reading(object):
    """Reading stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id",
        "moisture",
        "temperature"
    ]
    
    def __init__(self, plant_id, moisture, temperature):
        self.plant_id    = plant_id
        self.moisture    = moisture
        self.temperature = temperature

    @staticmethod
    def dict_to_reading(obj, ctx):
        return reading(
                obj['plant_id'],
                obj['moisture'],    
                obj['temperature'],    
            )

    @staticmethod
    def reading_to_dict(reading, ctx):
        return Reading.to_dict(reading)

    def to_dict(self):
        return dict(
                    plant_id    = self.plant_id,
                    moisture    = self.moisture,
                    temperature = self.temperature
                )