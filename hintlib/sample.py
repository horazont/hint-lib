import collections

from enum import Enum


class Part(Enum):
    CUSTOM_NOISE = 'noise'
    DS18B20 = 'ds18b20'  # temperature
    BME280 = 'bme280'  # environmental sensor
    TCS3200 = 'tcs3200'  # light sensor
    LSM303D = 'lsm303d'  # accelerometer + compass


class BME280Subpart(Enum):
    TEMPERATURE = 'temp'
    HUMIDITY = 'hum'
    PRESSURE = 'pres'


class TCS3200Subpart(Enum):
    RED = 'r'
    GREEN = 'g'
    BLUE = 'b'
    CLEAR = 'c'


class LSM303DSubpart(Enum):
    ACCEL_X = 'accel-x'
    ACCEL_Y = 'accel-y'
    ACCEL_Z = 'accel-z'
    COMPASS_X = 'compass-x'
    COMPASS_Y = 'compass-y'
    COMPASS_Z = 'compass-z'


class CustomNoiseSubpart(Enum):
    RMS = 'rms'
    MIN = 'min'
    MAX = 'max'


PART_SUBPARTS = {
    Part.BME280: BME280Subpart,
    Part.TCS3200: TCS3200Subpart,
    Part.LSM303D: LSM303DSubpart,
}


_SensorPath = collections.namedtuple(
    "_SensorPath",
    ["part", "instance", "subpart"]
)


class SensorPath(_SensorPath):
    def __new__(cls, part, instance, subpart=None):
        return super().__new__(cls, part, instance, subpart)

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)

    def __str__(self):
        parts = [self.instance]
        if isinstance(self.part, str):
            parts.insert(0, self.part)
        else:
            parts.insert(0, self.part.value)
        if self.subpart is not None:
            if isinstance(self.subpart, str):
                parts.append(self.subpart)
            else:
                parts.append(self.subpart.value)
        return "/".join(map(str, parts))


_Sample = collections.namedtuple(
    "_Sample",
    ["timestamp", "sensor", "value"]
)


class Sample(_Sample):
    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)

