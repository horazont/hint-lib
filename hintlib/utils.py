import calendar
import contextlib
import importlib
import os
import pathlib
import tempfile
import urllib.parse

from datetime import datetime


def escape_path(s):
    return urllib.parse.quote(s, safe=" ")


def unescape_path(s):
    return urllib.parse.unquote(s)


@contextlib.contextmanager
def safe_writer(destpath, mode="wb"):
    destpath = pathlib.Path(destpath)
    with tempfile.NamedTemporaryFile(
            mode=mode,
            dir=str(destpath.parent),
            prefix=".",
            delete=False) as tmpfile:
        try:
            yield tmpfile
        except:  # NOQA
            os.unlink(tmpfile.name)
            raise
        else:
            os.replace(tmpfile.name, str(destpath))


@contextlib.contextmanager
def extremely_safe_writer(destpath, mode="wb"):
    dirfd = os.open(str(destpath.parent), os.O_DIRECTORY)
    try:
        with safe_writer(destpath, mode) as f:
            yield f
            os.fsync(f.fileno())
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def write_file_safe(path, parts):
    dirfd = os.open(str(path.parent), os.O_DIRECTORY)
    try:
        with safe_writer(path, "xb") as f:
            f.writelines(parts)
            f.flush()
            os.fsync(f.fileno())
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def unpack_and_splice(buf, struct_obj):
    result = buf[struct_obj.size:]
    return result, struct_obj.unpack(buf[:struct_obj.size])


def unpack_all(buf, struct_obj, *, discard=False):
    size = struct_obj.size
    if len(buf) % size != 0 and not discard:
        raise ValueError(
            "buffer does not contain an integer number of structs"
        )
    return (
        struct_obj.unpack(buf[i*size:(i+1)*size])
        for i in range(len(buf)//size)
    )


def read_single(f, struct_obj):
    buf = bytearray()
    size = struct_obj.size
    while len(buf) < size:
        missing = size - len(buf)
        data = f.read(missing)
        if not data:
            raise EOFError
        buf.extend(data)
    result, = unpack_all(buf, struct_obj)
    return result


def read_all(f, struct_obj):
    while True:
        try:
            yield read_single(f, struct_obj)
        except EOFError:
            return


def write_single(f, struct_obj, *args):
    f.write(struct_obj.pack(*args))


def dt_to_ts(dt):
    return calendar.timegm(dt.utctimetuple())


def dt_to_ts_exact(dt):
    return calendar.timegm(dt.utctimetuple()) + dt.microsecond / 1e6


def decompose_dt(dt):
    return dt_to_ts(dt), dt.microsecond


def compose_dt(t_s, t_us):
    return datetime.utcfromtimestamp(t_s).replace(
        microsecond=t_us
    )


def kelvin_to_celsius(T):
    return T - 273.15


def celsius_to_kelvin(T):
    return T - kelvin_to_celsius(0)


class ExponentialBackOff:
    def __init__(self, base=2, start=1, max_=120):
        super().__init__()
        self.start = start
        self.max_ = max_
        self.base = base
        self._is_failing = False
        self._current = self.start

    def __iter__(self):
        return self

    def __next__(self):
        self._is_failing = True
        val = self._current
        self._current = min(self._current * self.base, self.max_)
        return val

    def next(self):
        return next(self)

    def reset(self):
        self._current = self.start

    @property
    def failing(self):
        return self._is_failing


def get_class_by_path(path, *, logger=None):
    logger = logger or logging.getLogger("__name__")

    module_name, class_ = path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        logger.error("failed to import plugin module %r",
                     module_name,
                     exc_info=True)
        raise ValueError("invalid class: {!r}".format(path))

    try:
        class_ = getattr(module, class_)
    except AttributeError:
        logger.error(
            "failed to find class %r in plugin module %r",
            class_,
            module_name,
            exc_info=True,
        )
        raise ValueError("invalid class: {!r}".format(path))

    return class_
