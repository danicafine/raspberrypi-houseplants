class Reading(object):
    __slots__ = [
        "plant_id",
        "moisture",
        "temperature"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/reading.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, plant_id, moisture, temperature):
        self.plant_id    = plant_id
        self.moisture    = moisture
        self.temperature = temperature

    @staticmethod
    def dict_to_reading(obj, ctx):
        return Reading(
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