class Mapping(object):
    __slots__ = [
        "sensor_id",
        "plant_id"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/mapping.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, sensor_id, plant_id):
        self.sensor_id = sensor_id
        self.plant_id  = plant_id

    @staticmethod
    def dict_to_mapping(obj, ctx=None):
        return Mapping(
                obj['sensor_id'],
                obj['plant_id']    
            )

    @staticmethod
    def mapping_to_dict(mapping, ctx=None):
        return Mapping.to_dict(mapping)

    def to_dict(self):
        return dict(
                    sensor_id = self.sensor_id,
                    plant_id  = self.plant_id
                )