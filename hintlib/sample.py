import array
import bz2
import collections
import dataclasses
import typing

from datetime import datetime, timedelta
from enum import Enum


class Part(Enum):
    CUSTOM_NOISE = 'noise'
    DS18B20 = 'ds18b20'  # temperature
    BME280 = 'bme280'  # environmental sensor
    TCS3200 = 'tcs3200'  # light sensor
    LSM303D = 'lsm303d'  # accelerometer + compass
    ESP8266_TX = 'esp8266-tx'
    SBX_I2C = 'sbx-i2c'  # STM32 core: I2C peripherial
    SBX_CPU = 'sbx-cpu'  # STM32 core: CPU metrics
    SBX_BME280 = 'sbx-bme280'  # STM32 core:


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


class ESP8266TXSubpart(Enum):
    SENT = 'sent'
    DROPPED = 'dropped'
    OOM_DROPPED = 'oom-dropped'
    ERROR = 'error'
    RETRANSMITTED = 'retransmitted'
    BROADCASTS = 'broadcasts'
    QUEUE_OVERRUN = 'queue-overrun'
    ACKLOCKS_NEEDED = 'acklocks-needed'


class CustomNoiseSubpart(Enum):
    RMS = 'rms'
    MIN = 'min'
    MAX = 'max'


class SBXI2CSubpart(Enum):
    TX_OVERRUNS = 'transaction-overruns'


class SBXBME280Subpart(Enum):
    CONFIGURE_STATUS = 'configure-status'
    TIMEOUTS = 'timeouts'


class SBXCPUSubpart(Enum):
    IDLE = 'idle'
    INTERRUPT_USART1 = 'intr-usart1'
    INTERRUPT_USART2 = 'intr-usart2'
    INTERRUPT_USART3 = 'intr-usart3'
    INTERRUPT_I2C1 = 'intr-i2c1'
    INTERRUPT_I2C2 = 'intr-i2c2'
    INTERRUPT_ADC = 'intr-adc'
    SCHEDULER = 'sched'
    INTERRUPT_USART1_DMA = 'intr-usart1-dma'
    INTERRUPT_USART2_DMA = 'intr-usart2-dma'
    INTERRUPT_USART3_DMA = 'intr-usart3-dma'
    INTERRUPT_I2C1_DMA = 'intr-i2c1-dma'
    INTERRUPT_I2C2_DMA = 'intr-i2c2-dma'
    INTERRUPT_ADC_DMA = 'intr-adc-dma'
    TASK_COMMTX = 'task-0'
    TASK_BLINK = 'task-1'
    TASK_STREAM_ACCEL_X = 'task-2'
    TASK_STREAM_ACCEL_Y = 'task-3'
    TASK_STREAM_ACCEL_Z = 'task-4'
    TASK_STREAM_COMPASS_X = 'task-5'
    TASK_STREAM_COMPASS_Y = 'task-6'
    TASK_STREAM_COMPASS_Z = 'task-7'
    TASK_SAMPLE_LIGHT = 'task-8'
    TASK_GENERATE_STATUS = 'task-9'
    TASK_SAMPLE_ONEWIRE = 'task-10'
    TASK_SAMPLE_ADC = 'task-11'
    TASK_SAMPLE_BME280 = 'task-12'


class SBXTXSubpart(Enum):
    MOST_ALLOCATED = 'most-allocated'


PART_SUBPARTS = {
    Part.BME280: BME280Subpart,
    Part.TCS3200: TCS3200Subpart,
    Part.LSM303D: LSM303DSubpart,
    Part.SBX_CPU: SBXCPUSubpart,
    Part.SBX_BME280: SBXBME280Subpart,
    Part.SBX_I2C: SBXI2CSubpart,
}


class SensorPath(typing.NamedTuple):
    module: str
    part: Part
    instance: str
    subpart: typing.Optional[str] = None

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)

    def __str__(self):
        parts = [self.module, self.instance]
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


class RawSample(typing.NamedTuple):
    timestamp: int
    sensor: SensorPath
    value: float

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)


class Sample(typing.NamedTuple):
    timestamp: datetime
    sensor: SensorPath
    value: float

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)


class SampleBatch(typing.NamedTuple):
    timestamp: datetime
    bare_path: SensorPath
    samples: typing.Mapping[str, float]

    def replace(self, *args, **kwargs):
        return self._replace(*args, **kwargs)

    def expand(self) -> typing.Iterable[Sample]:
        for subpart, value in self.samples.items():
            yield Sample(timestamp=self.timestamp,
                         path=self.bare_path.replace(subpart=subpart),
                         value=value)


class EncodedStreamData(typing.NamedTuple):
    data: bytes
    sample_type: str
    compressed: bool

    def decompress(self) -> "EncodedStreamData":
        if not self.compressed:
            return self
        return self._replace(data=bz2.compress(self.data),
                             compressed=True)

    def decode(self) -> "DecodedStreamData":
        decompressed = self.decompress()
        data = array.array(decompressed.sample_type)
        data.frombytes(decompressed.data)
        return DecodedStreamData(
            data=data,
        )

    def encode(self, compress: bool) -> "EncodedStreamData":
        if compress and not self.compressed:
            return self.compress()
        return self

    def compress(self) -> "EncodedStreamData":
        if self.compressed:
            return self
        return self._replace(data=bz2.decompress(self.data),
                             compressed=True)


class DecodedStreamData(typing.NamedTuple):
    data: array.array

    def decode(self) -> "DecodedStreamData":
        return self

    def encode(self, compress: bool) -> "EncodedStreamData":
        return EncodedStreamData(
            data=self.data.tobytes(),
            sample_type=self.data.typecode,
        ).encode(compress=compress)


@dataclasses.dataclass
class StreamBlock:
    timestamp: datetime
    path: SensorPath
    seq0: int
    period: timedelta
    range_: float
    data: typing.Union[DecodedStreamData, EncodedStreamData]

    def get_data(self) -> array.array:
        self._data = self._data.decode()
        return self._data.data

    def get_encoded_data(self, compress: bool) -> EncodedStreamData:
        self._data = self._data.encode(compress=compress)
        return self._data
