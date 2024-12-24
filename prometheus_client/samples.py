import datetime
from typing import Dict, NamedTuple, Optional, Sequence, Tuple, Union

from .prompb.metrics_pb2 import Bucket as PBBucket
from .prompb.metrics_pb2 import Counter as PBCounter
from .prompb.metrics_pb2 import Exemplar as PBExemplar
from .prompb.metrics_pb2 import Gauge as PBGauge
from .prompb.metrics_pb2 import Histogram as PBHistogram
from .prompb.metrics_pb2 import LabelPair as PBLabelPair
from .prompb.metrics_pb2 import Metric as PBMetric
from .prompb.metrics_pb2 import Summary as PBSummary
from .prompb.metrics_pb2 import Untyped as PBUntyped


class Timestamp:
    """A nanosecond-resolution timestamp."""

    def __init__(self, sec: float, nsec: float) -> None:
        if nsec < 0 or nsec >= 1e9:
            raise ValueError(f"Invalid value for nanoseconds in Timestamp: {nsec}")
        if sec < 0:
            nsec = -nsec
        self.sec: int = int(sec)
        self.nsec: int = int(nsec)

    def __str__(self) -> str:
        return f"{self.sec}.{self.nsec:09d}"

    def __repr__(self) -> str:
        return f"Timestamp({self.sec}, {self.nsec})"

    def __float__(self) -> float:
        return float(self.sec) + float(self.nsec) / 1e9

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Timestamp) and self.sec == other.sec and self.nsec == other.nsec

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __gt__(self, other: "Timestamp") -> bool:
        return self.nsec > other.nsec if self.sec == other.sec else self.sec > other.sec

    def __lt__(self, other: "Timestamp") -> bool:
        return self.nsec < other.nsec if self.sec == other.sec else self.sec < other.sec


# BucketSpan is experimental and subject to change at any time.
class BucketSpan(NamedTuple):
    offset: int
    length: int


# NativeHistogram is experimental and subject to change at any time.
class NativeHistogram(NamedTuple):
    count_value: float
    sum_value: float
    schema: int
    zero_threshold: float
    zero_count: float
    pos_spans: Optional[Tuple[BucketSpan, BucketSpan]] = None
    neg_spans: Optional[Tuple[BucketSpan, BucketSpan]] = None
    pos_deltas: Optional[Sequence[int]] = None
    neg_deltas: Optional[Sequence[int]] = None


# Timestamp and exemplar are optional.
# Value can be an int or a float.
# Timestamp can be a float containing a unixtime in seconds,
# a Timestamp object, or None.
# Exemplar can be an Exemplar object, or None.
class Exemplar(NamedTuple):
    labels: Dict[str, str]
    value: float
    timestamp: Optional[Union[float, Timestamp]] = None


class Sample(NamedTuple):
    name: str
    labels: Dict[str, str]
    value: float
    timestamp: Optional[Union[float, Timestamp]] = None
    exemplar: Optional[Exemplar] = None
    native_histogram: Optional[NativeHistogram] = None


def convert_exemplar(exemplar: Exemplar) -> PBExemplar:
    return PBExemplar(
        label=[PBLabelPair(name=name, value=value) for name, value in exemplar.labels.items()],
        value=exemplar.value,
        timestamp=exemplar.timestamp,  # alesieur: need type handler
    )


def make_label_pairs(label_names: Sequence[str], label_values: Sequence[str]) -> Sequence[PBLabelPair]:
    return [PBLabelPair(name=name, value=value) for name, value in zip(label_names, label_values)]


def convert_timestamp_to_ms(timestamp: Union[Timestamp, float]) -> int:
    return int(float(timestamp) * 1_000)


def make_untyped_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
    timestamp: Optional[Union[Timestamp, float]],
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        untyped=PBUntyped(value=value),
        timestamp_ms=convert_timestamp_to_ms(timestamp) if timestamp is not None else None,
    )


def make_counter_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
    timestamp: Optional[Union[Timestamp, float]],
    exemplar: Optional[Exemplar],
    created: Optional[float],
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        counter=PBCounter(
            value=value,
            exemplar=None,  # alesieur
            created_timestamp=datetime.datetime.fromtimestamp(created) if created is not None else None,
        ),
        timestamp_ms=convert_timestamp_to_ms(timestamp) if timestamp is not None else None,
    )


def make_gauge_metric(label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        gauge=PBGauge(value=value),
    )


def make_summary_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    sample_count: int,
    sample_sum: float,
    timestamp: Optional[Union[Timestamp, float]],
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        counter=PBSummary(
            sample_count=sample_count,
            sample_sum=sample_sum,
            # quantile=...,  # python doesn't expose quantiles for summaries
            # created_timestamp=...  # apparently we don't expose that either?
        ),
        timestamp_ms=convert_timestamp_to_ms(timestamp) if timestamp is not None else None,
    )


def make_bucket(
    cumulative_count: float,
    upper_bound: float | str,
    exemplar: Optional[Exemplar],
) -> PBBucket:
    return PBBucket(
        cumulative_count_float=cumulative_count,
        upper_bound=float(upper_bound),
        exemplar=convert_exemplar(exemplar) if exemplar is not None else None,
    )


def make_histogram_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    buckets: Sequence[PBBucket],
    sample_sum: Optional[float],
    timestamp: Optional[Union[Timestamp, float]],
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        histogram=PBHistogram(
            # Don't include sum and thus count if there's negative buckets.
            sample_count_float=buckets[-1].cumulative_count_float if buckets[0].upper_bound >= 0 else None,
            sample_sum=sample_sum if buckets[0].upper_bound >= 0 else None,
            bucket=buckets,
            # created_timestamp=...  # apparently we don't expose that either?s
        ),
        timestamp_ms=convert_timestamp_to_ms(timestamp) if timestamp is not None else None,
    )


def make_ghistogram_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    buckets: Sequence[PBBucket],
    sample_sum: Optional[float],
    timestamp: Optional[Union[Timestamp, float]],
) -> PBMetric:
    return PBMetric(
        label=make_label_pairs(label_names, label_values),
        histogram=PBHistogram(
            sample_count_float=buckets[-1].cumulative_count_float,
            sample_sum=sample_sum,
            bucket=buckets,
            # created_timestamp=...  # apparently we don't expose that either?
        ),
        timestamp_ms=convert_timestamp_to_ms(timestamp) if timestamp is not None else None,
    )
