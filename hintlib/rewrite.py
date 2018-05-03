import abc
import math


HI_C1 = -8.784695
HI_C2 = 1.61139411
HI_C3 = 2.338549
HI_C4 = -0.14611605
HI_C5 = -1.2308094e-2
HI_C6 = -1.6424828e-2
HI_C7 = 2.211732e-3
HI_C8 = 7.2546e-4
HI_C9 = -3.582e-6


PRESSURE_R_STAR = 287.05
KELVIN_OFFSET = 273.15
PRESSURE_C = 0.12
PRESSURE_A = 0.0065


def to_decibels_safe(value, min_):
    try:
        return 10 * math.log10(value)
    except ValueError:
        return min_


def heat_index(temperature, humidity, ignore_range=False):
    if temperature < 20 and not ignore_range:
        return None
    return (HI_C1 +
            HI_C2*temperature +
            HI_C3*humidity +
            HI_C4*temperature*humidity +
            HI_C5*temperature*temperature +
            HI_C6*humidity*humidity +
            HI_C7*temperature*temperature*humidity +
            HI_C8*humidity*humidity*temperature +
            HI_C9*humidity*humidity*temperature*temperature)


def height_correct_pressure(
        raw_pressure,
        temperature,
        humidity,
        g_0,
        height):
    abs_temperature = temperature + KELVIN_OFFSET
    humidity_norm = humidity/100
    temp_coeff = 6.112 * math.exp(17.62 * temperature/(243.12 + temperature))
    return raw_pressure * math.exp(
        g_0/(PRESSURE_R_STAR * (
            abs_temperature + PRESSURE_C * temp_coeff * humidity_norm +
            PRESSURE_A * height / 2
        ))*height
    )


CALC_REWRITE_GLOBALS = {
    "exp": math.exp,
    "to_decibels_safe": to_decibels_safe,
    "heat_index": heat_index,
    "height_correct_pressure": height_correct_pressure,
}


def _rewrite_instance(rule, logger):
    try:
        old_instance = rule["instance"]
        new_instance = rule["new_instance"]
        part = rule["part"]
    except KeyError as e:
        raise ValueError("rewrite rule needs key {!r}", str(e))

    def do_rewrite_instance(sample_obj):
        if (sample_obj.sensor.part == part and
                sample_obj.sensor.instance == old_instance):
            logger.debug("rewrote %r instance (%r -> %r)",
                         part,
                         old_instance,
                         new_instance)
            return sample_obj.replace(
                sensor=sample_obj.sensor.replace(
                    instance=new_instance
                )
            )
        return sample_obj

    logger.debug("built instance rewrite rule for %r: (%r -> %r)",
                 part,
                 old_instance,
                 new_instance)

    return do_rewrite_instance


def _rewrite_value_scale(rule, logger):
    try:
        part = rule["part"]
        factor = rule["factor"]
        subpart = rule.get("subpart")
    except KeyError as e:
        raise ValueError("rewrite rule needs key {!r}", str(e))

    def do_rewrite_value_scale(sample_obj):
        if (sample_obj.sensor.part == part and
                sample_obj.sensor.subpart == subpart):
            old_value = sample_obj.value
            new_value = old_value * factor
            logger.debug("rewrote %r value (%r -> %r)",
                         part,
                         old_value,
                         new_value)
            return sample_obj.replace(
                value=new_value
            )
        return sample_obj

    logger.debug("built value rewrite rule for %r: multiply with %r",
                 part,
                 factor)

    return do_rewrite_value_scale


class IndividualSampleRewriter:
    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        self.logger.debug("compiling individual rewrite rules: %r", config)
        self._rewrite_rules = [
            self._compile_rewrite_rule(rule, logger)
            for rule in config
        ]

    REWRITERS = {
        "instance": _rewrite_instance,
        "value-scale": _rewrite_value_scale,
    }

    def _compile_rewrite_rule(self, rule, logger):
        try:
            rewrite_builder = self.REWRITERS[rule["rewrite"]]
        except KeyError:
            raise ValueError(
                "missing 'rewrite' key in rewrite rule {!r}".format(rule)
            )

        return rewrite_builder(rule, logger)

    def rewrite(self, sample_obj):
        for rule in self._rewrite_rules:
            sample_obj = rule(sample_obj)
        return sample_obj


class CalcRewriteRule(metaclass=abc.ABCMeta):
    def __init__(self, rule, logger):
        super().__init__()
        self.logger = logger
        self.globals_ = CALC_REWRITE_GLOBALS.copy()
        try:
            self.part = rule["part"]
            self.subpart = rule["subpart"]
            self.instance = rule.get("instance")
            expression_src = rule["new_value"]
            precondition_src = rule.get("precondition")
        except KeyError as e:
            raise ValueError("rewrite rule needs key {!r}", str(e))

        self.expression = compile(
            expression_src,
            '<new_value of rewrite rule {!r}>'.format(rule),
            'eval'
        )

        if precondition_src is not None:
            self.precondition = compile(
                precondition_src,
                '<precondition of rewrite rule {!r}>'.format(rule),
                'eval'
            )
        else:
            self.precondition = None

        self.constants = rule.get("constants", {})

    def _match(self, sample_batch):
        ts, bare_path, samples = sample_batch
        if bare_path.part != self.part:
            return False
        if self.instance is not None and bare_path.instance != self.instance:
            return False
        return True

    def _prepare_locals(self, sample_batch):
        ts, bare_path, samples = sample_batch

        locals_ = dict(self.constants)
        for key, value in samples.items():
            locals_[key] = value

    def _check_precondition(self, locals_):
        try:
            precondition_result = eval(self.precondition,
                                       self.globals_,
                                       locals_)
        except NameError as exc:
            self.logger.warning("failed to evaluate precondition rule",
                                exc_info=True)
            return False

        if not precondition_result:
            return False

        return True

    def _evaluate(self, locals_):
        try:
            return eval(self.expression,
                        self.globals_,
                        locals_)
        except NameError as exc:
            self.logger.warning("failed to evaluate rewrite rule",
                                exc_info=True)
            return None

    @abc.abstractmethod
    def __call__(self, sample_batch):
        pass


class RewriteBatchValue(CalcRewriteRule):
    def __call__(self, sample_batch):
        ts, bare_path, samples = sample_batch

        if not self._match(sample_batch):
            return sample_batch
        locals_ = self._prepare_locals(sample_batch)
        if not self._check_precondition(locals_):
            self.logger.debug("precondition failed for %r", sample_batch)
            return sample_batch

        new_value = self._evaluate(locals_)

        new_samples = dict(samples)
        new_samples[self.subpart] = new_value

        self.logger.debug("rewrote %r value (%r -> %r)",
                          self.subpart,
                          samples.get(self.subpart),
                          new_value)

        return ts, bare_path, new_samples


class RewriteBatchCreate(CalcRewriteRule):
    def __call__(self, sample_batch):
        ts, bare_path, samples = sample_batch

        if not self._match(sample_batch):
            return sample_batch
        locals_ = self._prepare_locals(sample_batch)
        if not self._check_precondition(locals_):
            self.logger.debug("precondition failed for %r", sample_batch)
            return sample_batch

        new_value = self._evaluate(locals_)

        new_samples = dict(samples)
        new_samples[self.subpart] = new_value

        self.logger.debug("created %r value (%r -> %r)",
                          self.subpart,
                          new_value)

        return ts, bare_path, new_samples


class SampleBatchRewriter:
    def __init__(self, config, logger):
        super().__init__()
        self.logger = logger
        self.logger.debug("compiling batch rewrite rules: %r", config)
        self._rewrite_rules = [
            self._compile_rewrite_batch_rule(rule, logger)
            for rule in config
        ]

    REWRITERS = {
        "value": RewriteBatchValue,
        "create": RewriteBatchCreate,
    }

    def _compile_rewrite_batch_rule(self, batch_rule, logger):
        try:
            rewrite_builder_name = batch_rule["rewrite"]
        except KeyError:
            raise ValueError(
                "missing 'rewrite' key in rewrite rule {!r}".format(batch_rule)
            )

        try:
            rewrite_builder = self.REWRITERS[rewrite_builder_name]
        except KeyError:
            raise ValueError(
                "unknown rewriter: {!r}".format(rewrite_builder_name)
            )

        return rewrite_builder(batch_rule, logger)

    def rewrite(self, sample_batch):
        for rule in self._rewrite_rules:
            sample_batch = rule(sample_batch)
        return sample_batch
