from google.protobuf.internal.encoder import _VarintBytes  # type: ignore

from ..registry import CollectorRegistry, REGISTRY

CONTENT_TYPE_LATEST = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"


def generate_latest(registry: CollectorRegistry = REGISTRY) -> bytes:
    delimited_output = b""
    for metric in registry.collect():
        serialized_metric = metric.pb_mf.SerializeToString()
        delimited_output += _VarintBytes(len(serialized_metric))
        delimited_output += serialized_metric

    return delimited_output
