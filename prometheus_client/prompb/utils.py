from datetime import datetime
from typing import Iterable, Optional, Sequence, Tuple, Union

from google.protobuf.timestamp_pb2 import Timestamp as PBTimestamp

from .metrics_pb2 import (
    Bucket,
    Counter,
    Exemplar,
    Gauge,
    Histogram,
    LabelPair,
    Metric,
    Summary,
    Untyped,
)
from ..samples import Exemplar as ExemplarTuple
from ..samples import Sample, Timestamp
from ..utils import floatToGoString


def convert_timestamp_to_timestampms(timestamp: Optional[Union[Timestamp, float]]) -> Optional[int]:
    if isinstance(timestamp, Timestamp):
        return int(float(timestamp) // 1_000)
    elif isinstance(timestamp, float):
        return int(timestamp * 1_000)
    elif timestamp is None:
        return None
    raise ValueError(f"Invalid type for timestamp: {type(timestamp)}")


def convert_timestamp_to_pbtimestamp(timestamp: Optional[Union[Timestamp, float]]) -> Optional[PBTimestamp]:
    if isinstance(timestamp, Timestamp):
        ts = PBTimestamp()
        return ts.FromDatetime(datetime.fromtimestamp(float(timestamp)))
    elif isinstance(timestamp, float):
        ts = PBTimestamp()
        return ts.FromDatetime(datetime.fromtimestamp(timestamp))
    elif timestamp is None:
        return None
    raise ValueError(f"Invalid type for timestamp: {type(timestamp)}")


def convert_exemplar_to_pbexemplar(exemplar: Optional[ExemplarTuple]) -> Optional[Exemplar]:
    if isinstance(exemplar, ExemplarTuple):
        return Exemplar(
            label=[LabelPair(name=name, value=value) for name, value in exemplar.labels.items()],
            value=exemplar.value,
            timestamp=convert_timestamp_to_pbtimestamp(exemplar.timestamp)
        )
    elif exemplar is None:
        return None
    raise ValueError(f"Invalid type for exemplar: {type(exemplar)}")


def make_untyped_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
    timestamp: Optional[Union[Timestamp, float]] = None,
) -> Metric:
    return Metric(
        label=[LabelPair(name=k, value=v) for k, v in zip(label_names, label_values)],
        untyped=Untyped(value=value),
        timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    )


def make_counter_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
    timestamp: Optional[Union[Timestamp, float]] = None,
    exemplar: Optional[ExemplarTuple] = None,
    created: Optional[float] = None,
) -> Metric:
    return Metric(
        label=[LabelPair(name=k, value=v) for k, v in zip(label_names, label_values)],
        counter=Counter(
            value=value,
            exemplar=convert_exemplar_to_pbexemplar(exemplar),
            created_timestamp=convert_timestamp_to_pbtimestamp(created),
        ),
        timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    )


def make_gauge_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    value: float,
    timestamp: Optional[Union[Timestamp, float]] = None,
) -> Metric:
    return Metric(
        label=[LabelPair(name=k, value=v) for k, v in zip(label_names, label_values)],
        gauge=Gauge(
            value=value,
        ),
        timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    )


def make_summary_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    sample_count: int,
    sample_sum: float,
    timestamp: Optional[Union[Timestamp, float]] = None,
    created: Optional[float] = None,
) -> Metric:
    return Metric(
        label=[LabelPair(name=k, value=v) for k, v in zip(label_names, label_values)],
        summary=Summary(
            sample_count=sample_count,
            sample_sum=sample_sum,
            quantile=None,  # not exposed in python
            created_timestamp=convert_timestamp_to_pbtimestamp(created),
        ),
        timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    )


def make_histogram_metric(
    label_names: Sequence[str],
    label_values: Sequence[str],
    buckets: Sequence[Tuple[str, float]],  # (bound, count)
    sum_value: Optional[float],
    timestamp: Optional[Union[Timestamp, float]] = None,
    created: Optional[float] = None,
    gauge_histogram: bool = False,
) -> Metric:
    pb_buckets = []
    for bucket in buckets:
        bound, count = bucket
        pb_buckets.append(Bucket(cumulative_count_float=count, upper_bound=float(bound)))

    # Don't include sum and thus count if there's negative buckets.
    sample_count = None
    sample_sum = None
    if gauge_histogram or (float(buckets[0][0]) >= 0 and sum_value is not None):
        sample_count = buckets[-1][1]
        sample_sum = sum_value

    return Metric(
        label=[LabelPair(name=k, value=v) for k, v in zip(label_names, label_values)],
        histogram=Histogram(
            sample_count_float=sample_count,
            sample_sum=sample_sum,
            bucket=pb_buckets,
            created_timestamp=convert_timestamp_to_pbtimestamp(created),
        ),
        timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    )


EMPTY_EXEMPLAR = Exemplar()
EMPTY_TIMESTAMP = PBTimestamp()


def convert_pbexemplar_to_exemplar(exemplar: Exemplar) -> Optional[ExemplarTuple]:
    if exemplar == EMPTY_EXEMPLAR:
        return None
    return ExemplarTuple(
        labels={label.name: label.value for label in exemplar.label},
        value=exemplar.value,
        timestamp=exemplar.timestamp if exemplar.timestamp != EMPTY_TIMESTAMP else None,
    )


def convert_untyped_to_sample(name: str, metric: Untyped) -> Iterable[Sample]:
    return (
        Sample(
            name=name,
            labels={label.name: label.value for label in metric},
            value=metric.untyped.value,
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
    )


def convert_counter_to_sample(name: str, metric: Counter) -> Iterable[Sample]:
    labels = {label.name: label.value for label in metric}
    sample = Sample(
        name=name + "_total",
        labels=labels,
        value=metric.counter.value,
        timestamp=metric.timestamp_ms or None,
        exemplar=convert_pbexemplar_to_exemplar(metric.exemplar),
        native_histogram=None,
    )

    if metric.counter.created_timestamp == EMPTY_TIMESTAMP:
        return (sample,)
    return (
        sample,
        Sample(
            name=name + '_created',
            labels=labels,
            value=metric.counter.created_timestamp.ToDatetime().timestamp(),
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
    )


def convert_gauge_to_sample(name: str, metric: Gauge) -> Iterable[Sample]:
    return (
        Sample(
            name=name,
            labels={label.name: label.value for label in metric},
            value=metric.gauge.value,
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
    )


def convert_summary_to_sample(name: str, metric: Summary) -> Iterable[Sample]:
    labels = {label.name: label.value for label in metric}
    return (
        Sample(
            name=name + '_count',
            labels=labels,
            value=metric.summary.sample_count,
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
        Sample(
            name=name + '_sum',
            labels=labels,
            value=metric.summary.sample_sum,
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
        Sample(
            name=name + '_created',
            labels=labels,
            value=metric.summary.created_timestamp.ToDatetime().timestamp(),
            timestamp=metric.timestamp_ms or None,
            exemplar=None,
            native_histogram=None,
        ),
    )


def convert_histogram_to_sample(name: str, metric: Histogram, gauge_histogram: bool = False) -> Iterable[Sample]:
    samples = []
    labels = {label.name: label.value for label in metric}

    for bucket in metric.histogram.bucket:
        samples.append(
            Sample(
                name=name + '_bucket',
                labels=labels | {'le': floatToGoString(bucket.upper_bound)},
                value=bucket.cumulative_count_float,
                timestamp=metric.timestamp_ms or None,
                # alesieur: need to double check if bucket can still have exemplars or not
                exemplar=convert_pbexemplar_to_exemplar(bucket.exemplar),
                native_histogram=None,
            ),
        )

    # Don't include sum and thus count if there's negative buckets.
    if gauge_histogram or len(metric.bucket) > 0 and metric.bucket[0].upper_bound >= 0 and metric.sum_value >= 0:
        samples.append(
            Sample(
                name=name + '_count',
                labels=labels,
                value=metric.histogram.sample_count_float,
                timestamp=metric.timestamp_ms or None,
                exemplar=None,
                native_histogram=None,
            ),
        )
        samples.append(
            Sample(
                name=name + '_sum',
                labels=labels,
                value=metric.histogram.sample_sum,
                timestamp=metric.timestamp_ms or None,
                exemplar=None,
                native_histogram=None,
            ),
        )

    return samples
