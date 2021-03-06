import aioxmpp
import aioxmpp.xso
import aioxmpp.pubsub.xso

from aioxmpp.utils import namespaces

namespaces.hint_sensor = "https://xmlns.zombofant.net/hint/sensor/1.0"


class NumericSample(aioxmpp.xso.XSO):
    TAG = namespaces.hint_sensor, "numeric"

    subpart = aioxmpp.xso.Attr(
        "subpart",
        type_=aioxmpp.xso.String(),
        default=None,
    )

    value = aioxmpp.xso.Attr(
        "value",
        type_=aioxmpp.xso.Float(),
    )


class SampleBatch(aioxmpp.xso.XSO):
    TAG = namespaces.hint_sensor, "sample-batch"

    timestamp = aioxmpp.xso.Attr(
        "timestamp",
        type_=aioxmpp.xso.DateTime(),
    )

    part = aioxmpp.xso.Attr(
        "part"
    )

    instance = aioxmpp.xso.Attr(
        "instance",
        default=None,
    )

    samples = aioxmpp.xso.ChildList([NumericSample])


class SampleBatches(aioxmpp.xso.XSO):
    TAG = namespaces.hint_sensor, "sample-batches"

    module = aioxmpp.xso.Attr(
        "module",
    )

    batches = aioxmpp.xso.ChildList([SampleBatch])


class Stream(aioxmpp.xso.XSO):
    TAG = namespaces.hint_sensor, "stream"

    module = aioxmpp.xso.Attr(
        "module"
    )

    part = aioxmpp.xso.Attr(
        "part"
    )

    instance = aioxmpp.xso.Attr(
        "instance",
        default=None,
    )

    subpart = aioxmpp.xso.Attr(
        "subpart",
        default=None,
    )

    t0 = aioxmpp.xso.Attr(
        "t0",
        type_=aioxmpp.xso.DateTime()
    )

    period = aioxmpp.xso.Attr(
        "period",
        type_=aioxmpp.xso.Integer()
    )

    range_ = aioxmpp.xso.Attr(
        "range",
        type_=aioxmpp.xso.Float(),
    )

    sample_type = aioxmpp.xso.Attr(
        "type",
    )

    seq0 = aioxmpp.xso.Attr(
        "seq0",
        type_=aioxmpp.xso.Integer(),
    )

    data = aioxmpp.xso.Text(
        type_=aioxmpp.xso.Base64Binary()
    )


@aioxmpp.IQ.as_payload_class
class Query(aioxmpp.xso.XSO):
    TAG = namespaces.hint_sensor, "query"

    sample_batches = aioxmpp.xso.Child([SampleBatches])

    stream = aioxmpp.xso.Child([Stream])


aioxmpp.pubsub.xso.as_payload_class(SampleBatch)
