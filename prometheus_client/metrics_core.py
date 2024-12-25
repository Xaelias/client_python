from typing import Dict, Optional, Sequence, Tuple, Union

from .prompb.metrics_pb2 import Bucket as PBBucket
from .prompb.metrics_pb2 import MetricFamily as PBMetricFamily
from .prompb.metrics_pb2 import MetricType as PBMetricType
from .samples import (
    Exemplar, make_bucket, make_counter_metric, make_gauge_metric,
    make_ghistogram_metric, make_histogram_metric, make_summary_metric,
    make_untyped_metric, NativeHistogram, Timestamp,
)
from .validation import _validate_metric_name

METRIC_TYPES = (
    'counter', 'gauge', 'summary', 'histogram',
    'gaugehistogram', 'unknown', 'info', 'stateset',
)


class Metric:
    """A single metric family and its samples.

    This is intended only for internal use by the instrumentation client.

    Custom collectors should use GaugeMetricFamily, CounterMetricFamily
    and SummaryMetricFamily instead.
    """

    def __init__(self, name: str, documentation: str, typ: str, unit: str = ''):
        if unit and not name.endswith("_" + unit):
            name += "_" + unit
        _validate_metric_name(name)
        if typ not in METRIC_TYPES:
            raise ValueError('Invalid metric type: ' + typ)

        if typ == 'gaugehistogram':
            typ = 'gauge_histogram'
        pb_typ = getattr(PBMetricType, typ.upper(), PBMetricType.UNTYPED)

        self.name = name
        self.type = typ

        self.pb_mf = PBMetricFamily(
            name=name,
            help=documentation,
            type=pb_typ,  # type: ignore
            metric=[],
            unit=unit,
        )

    def add_sample(self, name: str, labels: Dict[str, str], value: float, timestamp: Optional[Union[Timestamp, float]] = None, exemplar: Optional[Exemplar] = None, native_histogram: Optional[NativeHistogram] = None) -> None:
        """Add a sample to the metric.

        Internal-only, do not use."""
        # alesieur: what do I do w/ this?
        # self.samples.append(Sample(name, labels, value, timestamp, exemplar, native_histogram))
        pass

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Metric) and self.pb_mf == other.pb_mf

    def __repr__(self) -> str:
        return "Metric({}, {}, {}, {}, {})".format(
            self.pb_mf.name,
            self.pb_mf.help,
            self.pb_mf.type,
            self.pb_mf.unit,
            # self.pb_mf.samples,  # alesieur: need to recursively __repr__ that
            ""
        )

    def _restricted_metric(self, names):
        """Build a snapshot of a metric with samples restricted to a given set of names."""
        samples = [s for s in self.pb_mf.metric if s.name in names]
        if samples:
            m = Metric(
                name=self.pb_mf.name,
                documentation=self.pb_mf.documentation,
                typ=self.pb_mf.type,
                unit=self.pb_mf.unit,
            )
            m.pb_mf.metric = samples
            return m
        return None


class UnknownMetricFamily(Metric):
    """A single unknown metric and its samples.
    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 unit: str = '',
                 ):
        Metric.__init__(self, name, documentation, 'unknown', unit)
        if labels is not None and value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if value is not None:
            self.add_metric([], value)

    def add_metric(self, labels: Sequence[str], value: float, timestamp: Optional[Union[Timestamp, float]] = None) -> None:
        """Add a metric to the metric family.
        Args:
        labels: A list of label values
        value: The value of the metric.
        """
        self.pb_mf.metric.append(
            make_untyped_metric(
                label_names=self._labelnames,
                label_values=labels,
                value=value,
                timestamp=timestamp
            )
        )


# For backward compatibility.
UntypedMetricFamily = UnknownMetricFamily


class CounterMetricFamily(Metric):
    """A single counter and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 created: Optional[float] = None,
                 unit: str = '',
                 exemplar: Optional[Exemplar] = None,
                 ):
        # Glue code for pre-OpenMetrics metrics.
        if name.endswith('_total'):
            name = name[:-6]
        Metric.__init__(self, name, documentation, 'counter', unit)
        if labels is not None and value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if value is not None:
            self.add_metric([], value, created, exemplar=exemplar)

    def add_metric(self,
                   labels: Sequence[str],
                   value: float,
                   created: Optional[float] = None,
                   timestamp: Optional[Union[Timestamp, float]] = None,
                   exemplar: Optional[Exemplar] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: The value of the metric
          created: Optional unix timestamp the child was created at.
        """
        self.pb_mf.metric.append(
            make_counter_metric(
                label_names=self._labelnames,
                label_values=labels,
                value=value,
                timestamp=timestamp,
                exemplar=exemplar,
                created=created
            )
        )
        if 'scrape_counts' in self.name:
            breakpoint()


class GaugeMetricFamily(Metric):
    """A single gauge and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 unit: str = '',
                 ):
        Metric.__init__(self, name, documentation, 'gauge', unit)
        if labels is not None and value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if value is not None:
            self.add_metric([], value)

    def add_metric(self, labels: Sequence[str], value: float, timestamp: Optional[Union[Timestamp, float]] = None) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A float
        """
        self.pb_mf.metric.append(
            make_gauge_metric(
                label_names=self._labelnames,
                label_values=labels,
                value=value
            )
        )


class SummaryMetricFamily(Metric):
    """A single summary and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 count_value: Optional[int] = None,
                 sum_value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 unit: str = '',
                 ):
        Metric.__init__(self, name, documentation, 'summary', unit)
        if (sum_value is None) != (count_value is None):
            raise ValueError('count_value and sum_value must be provided together.')
        if labels is not None and count_value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        # The and clause is necessary only for typing, the above ValueError will raise if only one is set.
        if count_value is not None and sum_value is not None:
            self.add_metric([], count_value, sum_value)

    def add_metric(self,
                   labels: Sequence[str],
                   count_value: int,
                   sum_value: float,
                   timestamp:
                   Optional[Union[float, Timestamp]] = None
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          count_value: The count value of the metric.
          sum_value: The sum value of the metric.
        """
        self.pb_mf.metric.append(
            make_summary_metric(
                label_names=self._labelnames,
                label_values=labels,
                sample_count=count_value,
                sample_sum=sum_value,
                timestamp=timestamp
            )
        )


class HistogramMetricFamily(Metric):
    """A single histogram and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 buckets: Optional[Sequence[Union[Tuple[str, float], Tuple[str, float, Exemplar]]]] = None,
                 sum_value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 unit: str = '',
                 ):
        Metric.__init__(self, name, documentation, 'histogram', unit)
        if sum_value is not None and buckets is None:
            raise ValueError('sum value cannot be provided without buckets.')
        if labels is not None and buckets is not None:
            raise ValueError('Can only specify at most one of buckets and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if buckets is not None:
            self.add_metric([], buckets, sum_value)

    def add_metric(self,
                   labels: Sequence[str],
                   buckets: Sequence[Union[Tuple[str, float], Tuple[str, float, Exemplar]]],
                   sum_value: Optional[float],
                   timestamp: Optional[Union[Timestamp, float]] = None) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of lists.
              Each inner list can be a pair of bucket name and value,
              or a triple of bucket name, value, and exemplar.
              The buckets must be sorted, and +Inf present.
          sum_value: The sum value of the metric.
        """
        pb_buckets = []
        for b in buckets:
            bucket, value = b[:2]
            exemplar = None
            if len(b) == 3:
                exemplar = b[2]  # type: ignore
            pb_buckets.append(make_bucket(value, bucket, exemplar))

        self.pb_mf.metric.append(
            make_histogram_metric(
                label_names=self._labelnames,
                label_values=labels,
                buckets=pb_buckets,
                sample_sum=sum_value,
                timestamp=timestamp
            )
        )


class GaugeHistogramMetricFamily(Metric):
    """A single gauge histogram and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 buckets: Optional[Sequence[Tuple[str, float]]] = None,
                 gsum_value: Optional[float] = None,
                 labels: Optional[Sequence[str]] = None,
                 unit: str = '',
                 ):
        Metric.__init__(self, name, documentation, 'gaugehistogram', unit)
        if labels is not None and buckets is not None:
            raise ValueError('Can only specify at most one of buckets and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if buckets is not None:
            self.add_metric([], buckets, gsum_value)

    def add_metric(self,
                   labels: Sequence[str],
                   buckets: Sequence[Tuple[str, float]],
                   gsum_value: Optional[float],
                   timestamp: Optional[Union[float, Timestamp]] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of pairs of bucket names and values.
              The buckets must be sorted, and +Inf present.
          gsum_value: The sum value of the metric.
        """

        self.pb_mf.metric.append(
            make_ghistogram_metric(
                label_names=self._labelnames,
                label_values=labels,
                buckets=[PBBucket(cumulative_count_float=value, upper_bound=float(bucket)) for bucket, value in buckets],
                sample_sum=gsum_value,
                timestamp=timestamp,
            )
        )


class InfoMetricFamily(Metric):
    """A single info and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 value: Optional[Dict[str, str]] = None,
                 labels: Optional[Sequence[str]] = None,
                 ):
        Metric.__init__(self, name, documentation, 'info')
        if labels is not None and value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if value is not None:
            self.add_metric([], value)

    def add_metric(self,
                   labels: Sequence[str],
                   value: Dict[str, str],
                   timestamp: Optional[Union[Timestamp, float]] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A dict of labels
        """
        self.pb_mf.metric.append(
            make_untyped_metric(
                label_names=self._labelnames,
                label_values=labels,
                value=1,
                timestamp=timestamp
            )
        )


class StateSetMetricFamily(Metric):
    """A single stateset and its samples.

    For use by custom collectors.
    """

    def __init__(self,
                 name: str,
                 documentation: str,
                 value: Optional[Dict[str, bool]] = None,
                 labels: Optional[Sequence[str]] = None,
                 ):
        Metric.__init__(self, name, documentation, 'stateset')
        if labels is not None and value is not None:
            raise ValueError('Can only specify at most one of value and labels.')
        if labels is None:
            labels = []
        self._labelnames = tuple(labels)
        if value is not None:
            self.add_metric([], value)

    def add_metric(self,
                   labels: Sequence[str],
                   value: Dict[str, bool],
                   timestamp: Optional[Union[Timestamp, float]] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A dict of string state names to booleans
        """
        labels = tuple(labels)
        for state, enabled in sorted(value.items()):
            self.pb_mf.metric.append(
                make_untyped_metric(
                    label_names=self._labelnames + (self.pb_mf.name,),
                    label_values=labels + (state,),
                    value=1 if enabled else 0,
                    timestamp=timestamp
                )
            )
