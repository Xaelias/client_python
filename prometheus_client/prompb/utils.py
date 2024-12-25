from datetime import datetime
from typing import Optional, Sequence, Tuple, Union

from .prompb.metrics_pb2 import (
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
from .samples import Exemplar as ExemplarTuple
from .samples import Timestamp


def convert_timestamp_to_timestampms(timestamp: Optional[Union[Timestamp, float]]) -> int:
    if isinstance(timestamp, Timestamp):
        return float(timestamp) // 1_000
    elif isinstance(timestamp, float):
        return int(timestamp * 1_000)
    elif timestamp is None:
        return None
    raise ValueError(f"Invalid type for timestamp: {type(timestamp)}")


def convert_timestamp_to_pbtimestamp(timestamp: Optional[Union[Timestamp, float]]) -> datetime:
    if isinstance(timestamp, Timestamp):
        return datetime.fromtimestamp(float(timestamp))
    elif isinstance(timestamp, float):
        return datetime.fromtimestamp(timestamp)
    elif timestamp is None:
        return None
    raise ValueError(f"Invalid type for timestamp: {type(timestamp)}")


def convert_exemplar_to_pbexemplar(exemplar: ExemplarTuple) -> Exemplar:
    return Exemplar(
        label=[LabelPair(name=name, value=value) for name, value in exemplar.labels.items()],
        value=exemplar.value,
        timestamp=convert_timestamp_to_pbtimestamp(exemplar.timestamp)
    )


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
        bound, count = bucket[:2]
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
