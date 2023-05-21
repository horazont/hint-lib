import asyncio
import contextlib
import math

from datetime import datetime, timedelta

import aiohttp

import lxml.etree as etree

import hintlib.cache

from . import common


def _get_child_attr(parent, tag,
                    attr="value",
                    *,
                    cast=float,
                    bias=None,
                    scale=None):
    elem = parent.find(tag)
    if elem is None:
        return None
    value = elem.get(attr)
    if value is None:
        return None
    value = cast(value)
    if scale is not None:
        value *= scale
    if bias is not None:
        value += bias
    return value


def _parse_datetime(s):
    return datetime.strptime(
        s,
        "%Y-%m-%dT%H:%M:%SZ"
    )


def process_datapoint(parent, ts):
    datapoint = common.Timepoint(ts)

    datapoint.temperature = _get_child_attr(
        parent,
        "temperature",
        "value",
        bias=273.15
    )

    datapoint.wind_speed = _get_child_attr(
        parent,
        "windSpeed",
        "mps",
        scale=0.44704,
    )

    datapoint.wind_bearing = _get_child_attr(
        parent,
        "windDirection",
        "deg",
        scale=math.pi/180
    )

    datapoint.humidity = _get_child_attr(
        parent,
        "humidity",
        "value",
        scale=0.01,
    )

    datapoint.pressure = _get_child_attr(
        parent,
        "pressure",
        "value",
    )

    datapoint.fog = _get_child_attr(
        parent,
        "fog",
        "percent",
        scale=0.01,
    )

    datapoint.cloud_cover = _get_child_attr(
        parent,
        "cloudiness",
        "percent",
        scale=0.01,
    )

    datapoint.cloud_cover_low = _get_child_attr(
        parent,
        "lowClouds",
        "percent",
        scale=0.01,
    )

    datapoint.cloud_cover_mid = _get_child_attr(
        parent,
        "mediumClouds",
        "percent",
        scale=0.01,
    )

    datapoint.cloud_cover_high = _get_child_attr(
        parent,
        "highClouds",
        "percent",
        scale=0.01,
    )

    datapoint.dewpoint_temperature = _get_child_attr(
        parent,
        "dewpointTemperature",
        "value",
        bias=273.15
    )

    return datapoint


def process_interval(parent, start, end):
    interval = common.Interval(
        start, end
    )

    interval.precipitation = _get_child_attr(
        parent,
        "precipitation",
        "value",
    )

    interval.precipitation_min = _get_child_attr(
        parent,
        "precipitation",
        "minvalue",
    )

    interval.precipitation_max = _get_child_attr(
        parent,
        "precipitation",
        "maxvalue",
    )

    return interval


class Requester(hintlib.cache.AdvancedHTTPRequester):
    URL = "https://api.met.no/weatherapi/locationforecast/2.0/classic"

    def __init__(self,
                 mock_data=None,
                 altitude=None,
                 dump=None, **kwargs):
        super().__init__(**kwargs)
        self.max_cache_over_expiry = timedelta(minutes=45)
        self.mock_data = mock_data
        self.dump = dump
        self.altitude = altitude

    def _is_too_stale(self, cache_entry):
        age = datetime.utcnow() - cache_entry.expires
        return age > self.max_cache_over_expiry

    def _get_backing_off_result(self, expired_cache_entry=None, **kwargs):
        # if the cache entry is not 'too old', return it, otherwise make it
        # explicit that we’re currently backing off.

        if expired_cache_entry is not None:
            if self._is_too_stale(expired_cache_entry):
                return None

        return expired_cache_entry

    def _decode_response_into(self, data, cache_entry):
        if self.dump is not None:
            with open(self.dump, "wb") as f:
                f.write(data)

        try:
            tree = etree.fromstring(data)
        except ValueError as err:
            raise hintlib.cache.RequestError(
                str(err),
                back_off=False,
            ) from err

        datapoints = []
        intervals = []

        for time in tree.xpath("//time"):
            start = _parse_datetime(time.get("from"))
            end = _parse_datetime(time.get("to"))
            if start == end:
                datapoints.append(process_datapoint(time[0], start))
            else:
                intervals.append(process_interval(time[0], start, end))

        datapoints.sort(
            key=lambda x: x.timestamp
        )

        intervals.sort(
            key=lambda x: (x.start, x.end)
        )

        cache_entry.data = (
            datapoints,
            intervals
        )

    async def _perform_http_request(self, session, lat, lon,
                                    expired_cache_entry=None):
        if self.mock_data is not None:
            self.logger.warning("using mock data")
            cache_entry = expired_cache_entry or \
                hintlib.cache.CacheEntry()
            self._decode_response_into(
                self.mock_data,
                cache_entry
            )
            return cache_entry

        headers = []
        if expired_cache_entry is not None:
            if expired_cache_entry.last_modified is not None:
                headers.append(
                    ("If-Modified-Since",
                        expired_cache_entry.last_modified)
                )

        response = None
        params = {
            "lat": str(lat),
            "lon": str(lon),
        }
        if self.altitude:
            params["altitude"] = str(self.altitude)
        try:
            async with session.get(
                    self.URL,
                    params=params,
                    headers=headers) as response:
                self.logger.debug(
                    "response: '%d: %s' (headers=%r)",
                    response.status,
                    response.reason,
                    response.headers,
                )

                if response.status == 304:
                    # cache is still valid
                    self.logger.info(
                        "re-using cached data (received 304)"
                    )
                    return expired_cache_entry

                elif response.status != 200:
                    raise hintlib.cache.RequestError(
                        response.reason,
                        back_off=False,
                        cache_entry=None
                    )

                last_modified = response.headers.get("Last-Modified")
                data = await response.read()

        except aiohttp.ClientResponseError:
            if response.status == 304:
                # some problem with aiohttp?
                return expired_cache_entry
            else:
                raise
        except asyncio.TimeoutError:
            cache_entry = expired_cache_entry
            if cache_entry is not None and self._is_too_stale(cache_entry):
                cache_entry = None

            raise hintlib.cache.RequestError(
                "timeout",
                back_off=True,
                cache_entry=cache_entry,
            )
        else:
            cache_entry = expired_cache_entry or hintlib.cache.CacheEntry()

        self._decode_response_into(data, cache_entry)
        cache_entry.last_modified = last_modified
        cache_entry.expires = datetime.utcnow() + timedelta(minutes=1)

        return cache_entry


class Service:
    DESCRIPTION = (
        "api.met.no/weatherapi is an interface to a selection of data "
        "produced by MET Norway"
    )
    DEFAULT_LICENSE = "CC BY 3.0"

    def __init__(self, config):
        super().__init__()
        mock_data = None
        try:
            mock_data_file = config["mock_data"]
        except KeyError:
            pass
        else:
            with open(mock_data_file, "rb") as f:
                mock_data = f.read()

        self.requester = Requester(
            mock_data=mock_data,
            dump=config.get("dump"),
            altitude=config.get("altitude"),
        )

    async def get_data(self, lat, lon):
        data = await self.requester.request(
            lat=lat,
            lon=lon,
        )

        return data
