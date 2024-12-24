# import base64
# from contextlib import closing
# import gzip
# from http.server import BaseHTTPRequestHandler
# import os
# import socket
# from socketserver import ThreadingMixIn
# import ssl
# import sys
# import threading
# from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
# from urllib.error import HTTPError
# from urllib.parse import parse_qs, quote_plus, urlparse
# from urllib.request import (
#     BaseHandler, build_opener, HTTPHandler, HTTPRedirectHandler, HTTPSHandler,
#     Request,
# )
# from wsgiref.simple_server import make_server, WSGIRequestHandler, WSGIServer

# from .openmetrics import exposition as openmetrics
import datetime

from google.protobuf.internal.encoder import _VarintBytes

from ..metrics_core import CounterMetricFamily, GaugeMetricFamily
from ..registry import REGISTRY, CollectorRegistry
from .metrics_pb2 import (
    Bucket,
    Counter,
    Exemplar,
    Gauge,
    Histogram,
    LabelPair,
    Metric,
    MetricFamily,
    MetricType,
    Summary,
)

# from .utils import floatToGoString
# from .validation import _is_valid_legacy_metric_name

CONTENT_TYPE_LATEST = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited; escaping=values"


def generate_counter_mf(metric: CounterMetricFamily) -> MetricFamily:
    metrics = []
    for sample in metric.samples:
        sname = sample.name
        if sname.endswith("_created"):
            metrics[-1].counter.created_timestamp = datetime.datetime.fromtimestamp(
                sample.value
            )
            continue

        labels = [
            LabelPair(name=name, value=value) for name, value in sample.labels.items()
        ]
        metrics.append(
            Metric(
                label=labels,
                counter=Counter(
                    value=sample.value,
                    exemplar=Exemplar(
                        labels=labels,
                        value=sample.exemplar.value,
                        timestamp=sample.exemplar.timestamp,
                    )
                    if sample.exemplar
                    else None,
                ),
                timestamp_ms=sample.timestamp,
            )
        )

    return MetricFamily(
        name=metric.name,
        help=metric.documentation,
        type=MetricType.COUNTER,
        metric=metrics,
    )


def generate_gauge_mf(metric: GaugeMetricFamily) -> MetricFamily:
    metrics = []
    for sample in metric.samples:
        labels = [
            LabelPair(name=name, value=value) for name, value in sample.labels.items()
        ]
        metrics.append(
            Metric(
                label=labels,
                gauge=Gauge(value=sample.value),
                timestamp_ms=sample.timestamp,
            )
        )

    return MetricFamily(
        name=metric.name,
        help=metric.documentation,
        type=MetricType.GAUGE,
        metric=metrics,
    )


def generate_summary_mf(metric: CounterMetricFamily) -> MetricFamily:
    metrics = []
    for sample in metric.samples:
        sname = sample.name
        # this is entirely reliant on the rest of the client exposing these in order
        if sname.endswith("_sum"):
            metrics[-1].summary.sample_sum = sample.value
            continue
        if sname.endswith("_created"):
            metrics[-1].summary.created_timestamp = datetime.datetime.fromtimestamp(
                sample.value
            )
            continue
        labels = [
            LabelPair(name=name, value=value) for name, value in sample.labels.items()
        ]
        metrics.append(
            Metric(
                label=labels,
                summary=Summary(sample_count=int(sample.value)),
                timestamp_ms=sample.timestamp,
            )
        )

    return MetricFamily(
        name=metric.name,
        help=metric.documentation,
        type=MetricType.SUMMARY,
        metric=metrics,
    )


def generate_histogram_m(
    labels: dict[str, str],
    sample_count: float,
    sample_sum: float,
    les: dict[float, float],
    created_at: float,
):
    labels = [
        LabelPair(name=name, value=value)
        for name, value in labels.items()
        if name != "le"
    ]
    return Metric(
        label=labels,
        histogram=Histogram(
            sample_count_float=sample_count,
            sample_sum=sample_sum,
            bucket=[
                Bucket(
                    cumulative_count_float=les[le],
                    upper_bound=le,
                )
                for le in sorted(les.keys())
            ],
            created_timestamp=created_at,
        ),
    )


def generate_histogram_mf(metric: CounterMetricFamily) -> MetricFamily:
    metrics = []

    current_les = {}
    current_sum = None
    current_created = None
    current_count = None
    for sample in metric.samples:
        print(sample)
        sname = sample.name
        if sname.endswith("_sum"):
            current_sum = sample.value
        if sname.endswith("_count"):
            current_count = sample.value
        if sname.endswith("_bucket"):
            le = float(sample.labels["le"])
            if le in current_les:
                metrics.append(
                    generate_histogram_m(
                        sample.labels,
                        current_count,
                        current_sum,
                        current_les,
                        current_created,
                    )
                )
                current_les = {}
                current_sum = None
                current_created = None
                current_count = None
            else:
                current_les[le] = sample.value
        if sname.endswith("_created"):
            current_created = datetime.datetime.fromtimestamp(sample.value)

    if current_les:
        metrics.append(
            generate_histogram_m(
                sample.labels, current_count, current_sum, current_les, current_created
            )
        )

    return MetricFamily(
        name=metric.name,
        help=metric.documentation,
        type=MetricType.HISTOGRAM,
        metric=metrics,
    )


def generate_latest(registry: CollectorRegistry = REGISTRY) -> bytes:
    output = []
    for metric in registry.collect():
        try:
            match metric.type:
                case "counter":
                    output.append(generate_counter_mf(metric))
                case "gauge":
                    output.append(generate_gauge_mf(metric))
                case "summary":
                    output.append(generate_summary_mf(metric))
                case "histogram":
                    output.append(generate_histogram_mf(metric))
                case _:
                    raise ValueError(f"Unknown metric type {metric.type}")
        except Exception as exception:
            exception.args = (exception.args or ("",)) + (metric,)
            raise

    delimited_output = b""
    print(f"output length: {len(output)}")
    for metric in output:
        serialized_metric = metric.SerializeToString()
        # delimited_output += struct.pack('<i', len(serialized_metric))
        delimited_output += _VarintBytes(len(serialized_metric))
        delimited_output += serialized_metric
        print(f"serialized {metric.name}#{metric.type}")

    return delimited_output
