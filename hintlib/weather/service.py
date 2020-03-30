import collections
import logging
import math
import typing

from datetime import datetime

from . import common


class AggregatedPoint(typing.NamedTuple):
    avg: float
    min_: float
    max_: float
    min_t: typing.Optional[datetime] = None
    max_t: typing.Optional[datetime] = None


class AggregatedAngle(typing.NamedTuple):
    avg: float


class AggregatedPrecipitation(typing.NamedTuple):
    sum_: float
    min_sum: float
    max_sum: float
    type_: typing.Optional[common.PrecipitationType] = None


class AggregatedInterval(typing.NamedTuple):
    start: datetime
    end: datetime

    apparent_temperature: typing.Optional[AggregatedPoint] = None
    cloud_cover: typing.Optional[AggregatedPoint] = None
    cloud_cover_low: typing.Optional[AggregatedPoint] = None
    cloud_cover_mid: typing.Optional[AggregatedPoint] = None
    cloud_cover_high: typing.Optional[AggregatedPoint] = None
    dewpoint_temperature: typing.Optional[AggregatedPoint] = None
    humidity: typing.Optional[AggregatedPoint] = None
    fog: typing.Optional[AggregatedPoint] = None
    ozone: typing.Optional[AggregatedPoint] = None
    precipitation: typing.Optional[AggregatedPoint] = None
    precipitation_probability: typing.Optional[float] = None
    pressure: typing.Optional[AggregatedPoint] = None
    visibility: typing.Optional[AggregatedPoint] = None
    temperature: typing.Optional[AggregatedPoint] = None
    wind_bearing: typing.Optional[AggregatedAngle] = None
    wind_speed: typing.Optional[AggregatedPoint] = None



def aggregate_avg(items, attrname):
    sum_ = 0
    min_ = None
    max_ = None

    count = 0
    for item in items:
        value = getattr(item, attrname)
        if value is None:
            continue

        sum_ += value
        count += 1
        min_ = value if min_ is None else min(min_, value)
        max_ = value if max_ is None else max(max_, value)

    if count == 0:
        return None

    return AggregatedPoint(avg=sum_/count, min_=min_, max_=max_)


def aggregate_probability(items, attrname):
    count = 0
    accum = 1.0

    for item in items:
        prob = getattr(item, attrname)
        if prob is None:
            continue

        count += 1
        accum *= 1 - prob

    if count == 0:
        return None

    return 1 - accum


def aggregate_angle(items, attrname):
    sum_sin = 0
    sum_cos = 0

    count = 0
    for item in items:
        value = getattr(item, attrname)
        if value is None:
            continue

        sum_sin += math.sin(value)
        sum_cos += math.cos(value)
        count += 1

    if count == 0:
        return None

    result = {
        "avg": math.atan2(sum_sin/count, sum_cos/count),
    }

    return AggregatedAngle(**result)


def aggregate_sum(items, attrname):
    sum_ = 0

    count = 0
    for item in items:
        value = getattr(item, attrname)
        if value is None:
            continue

        sum_ += value
        count += 1

    if count == 0:
        return None

    return sum_


def aggregate_precipitation(items):
    types = collections.Counter()
    best_type = None
    for item in items:
        if item.precipitation_type is None:
            continue
        types[item.precipitation_type] += 1

    if types:
        best_type = max(types.items(), key=lambda x: x[1])[0]

    sum_ = aggregate_sum(items, "precipitation")
    min_sum = aggregate_sum(items, "precipitation_min")
    max_sum = aggregate_sum(items, "precipitation_max")

    return AggregatedPrecipitation(
        sum_=sum_,
        min_sum=min_sum,
        max_sum=max_sum,
        type_=best_type,
    )


def select_intervals(start, end, intervals):
    if not intervals:
        return [], False

    start_candidates = [
        interval
        for interval in intervals
        if interval.start == intervals[0].start
    ]
    start_candidates.reverse()

    best_candidate_loss = None
    best_candidate = []

    for start_interval in start_candidates:
        chain = [start_interval]
        prev = start_interval.end

        for interval in intervals:
            if interval.start == prev and interval.end <= end:
                chain.append(interval)
                prev = interval.end
                if prev == end:
                    return chain, chain[0].start == start

        loss = (
            abs((chain[0].start - start).total_seconds()) +
            abs((chain[-1].end - end).total_seconds())
        )

        if best_candidate_loss is None or best_candidate_loss > loss:
            best_candidate_loss = loss
            best_candidate = chain

    return best_candidate, False


async def interval_from_source(backend, lat, lon, start, end):
    logger = logging.getLogger(__name__ + ".interval_from_source")
    into = common.Interval(start, end)

    logger.debug(
        "collecting data from %r for location (%.6f, %.6f) within "
        "[%s, %s]",
        backend,
        lat, lon,
        start, end
    )

    datapoints, intervals = await backend.get_data(
        lat, lon,
    )

    datapoints = [
        item for item in datapoints
        if start <= item.timestamp <= end
    ]

    if intervals:
        logger.debug(
            "first interval [%s, %s]",
            intervals[0].start,
            intervals[0].end
        )

    intervals = [
        item for item in intervals
        if start <= item.start < end and start < item.end <= end
    ]

    logger.debug(
        "%d datapoint candidates, %d interval candidates",
        len(datapoints),
        len(intervals)
    )

    intervals, accurate = select_intervals(
        start,
        end,
        intervals
    )

    logger.debug(
        "using intervals %r (accurate=%r)",
        intervals,
        accurate
    )

    into.apparent_temperature = aggregate_avg(
        datapoints,
        "apparent_temperature",
    )

    into.dewpoint_temperature = aggregate_avg(
        datapoints,
        "dewpoint_temperature",
    )

    into.temperature = aggregate_avg(
        datapoints,
        "temperature",
    )

    into.fog = aggregate_avg(
        datapoints,
        "fog",
    )

    into.humidity = aggregate_avg(
        datapoints,
        "humidity",
    )

    into.ozone = aggregate_avg(
        datapoints,
        "ozone",
    )

    into.pressure = aggregate_avg(
        datapoints,
        "pressure",
    )

    into.visibility = aggregate_avg(
        datapoints,
        "visibility",
    )

    into.wind_speed = aggregate_avg(
        datapoints,
        "wind_speed",
    )

    into.wind_bearing = aggregate_angle(
        datapoints,
        "wind_bearing",
    )

    for type_ in [
            "low",
            "mid",
            "high",
            None]:
        attr = "cloud_cover"
        if type_ is not None:
            attr += "_" + type_

        item = aggregate_avg(
            datapoints,
            attr,
        )
        if item is None:
            continue

        suffix = type_ if type_ is not None else ""
        setattr(into, "cloud_cover{}".format(suffix), item)

    into.precipitation = aggregate_precipitation(intervals)

    into.precipitation_probability = aggregate_probability(
        intervals,
        "precipitation_probability"
    )

    return into


async def intervals_from_source(backend, lat, lon, intervals):
    result = []

    for start, end in intervals:
        result.append(
            await interval_from_source(backend, lat, lon, start, end)
        )

    return result
