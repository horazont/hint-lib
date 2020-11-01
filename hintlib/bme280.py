import struct


Dig88 = struct.Struct(
    "<"
    "HhhHhhhhhhhhxB"
)

Dige1 = struct.Struct(
    "<"
    "hBbBbb"
)


def get_calibration(dig88, dige1):
    values = Dig88.unpack(dig88)

    dig_H2, dig_H3, dig_H45_1, dig_H45_2, dig_H45_3, dig_H6 = Dige1.unpack(
        dige1
    )
    values += dig_H2, dig_H3

    dig_H4 = (dig_H45_1 << 4) | (dig_H45_2 & 0xf)
    dig_H5 = (dig_H45_3 << 4) | ((dig_H45_2 >> 4) & 0xf)

    values += dig_H4, dig_H5, dig_H6

    return values


def get_readout(readout):
    pressure_raw = ((readout[0] << 16) | (readout[1] << 8) | readout[2]) >> 4
    temp_raw = ((readout[3] << 16) | (readout[4] << 8) | readout[5]) >> 4
    humidity_raw = struct.unpack(">H", readout[6:8])[0]
    return temp_raw, pressure_raw, humidity_raw


def compensate_temperature(calibration, raw):
    dig_T1, dig_T2, dig_T3 = calibration[:3]

    UT = raw
    var1 = (UT / 16384 - dig_T1 / 1024) * dig_T2
    var2 = ((UT / 131072 - dig_T1 / 8192) * (
            UT / 131072 - dig_T1 / 8192)) * dig_T3
    return (var1 + var2) / 5120


def compensate_pressure(calibration, raw, temp):
    dig_P1, dig_P2, dig_P3, dig_P4, dig_P5, dig_P6, dig_P7, dig_P8, dig_P9 = \
        calibration[3:12]

    adc = raw
    t_fine = int(temp * 5120)
    var1 = t_fine / 2 - 64000
    var2 = var1 * var1 * dig_P6 / 32768
    var2 = var2 + var1 * dig_P5 * 2
    var2 = var2 / 4 + dig_P4 * 65536
    var1 = (
        dig_P3 * var1 * var1 / 524288 +
        dig_P2 * var1
    ) / 524288
    var1 = (1 + var1 / 32768) * dig_P1
    if var1 == 0:
        return 0
    p = 1048576 - adc
    p = ((p - var2 / 4096) * 6250) / var1
    var1 = dig_P9 * p * p / 2147483648
    var2 = p * dig_P8 / 32768
    p = p + (var1 + var2 + dig_P7) / 16
    return p


def compensate_humidity(calibration, raw, temp):
    dig_H1, dig_H2, dig_H3, dig_H4, dig_H5, dig_H6 = \
        calibration[12:]

    adc = raw
    t_fine = int(temp * 5120)
    h = t_fine - 76800
    h = (
        (adc - (dig_H4 * 64 + dig_H5 / 16384 * h)) *
        (dig_H2 / 65536 * (1 + dig_H6 / 67108864 * h *
                           (1 + dig_H3 / 67108864 * h)))
    )
    h = h * (1 - dig_H1 * h / 524288)
    return h
