from typing import Dict, Optional, Sequence, Tuple, Union

from .prompb.metrics_pb2 import Exemplar as Exemplar
from .prompb.metrics_pb2 import LabelPair as PBLabelPair
from .prompb.metrics_pb2 import Metric as PBMetric
from .prompb.metrics_pb2 import MetricFamily as PBMetricFamily
from .prompb.metrics_pb2 import MetricType as PBMetricType
from .prompb.metrics_pb2 import Untyped as PBUntyped
from .prompb.utils import convert_timestamp_to_timestampms
from .prompb.utils import make_counter_metric, make_gauge_metric, make_histogram_metric, make_summary_metric, make_untyped_metric
from .samples import Timestamp
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

        if typ == 'untyped':
            typ = 'unknown'
        if typ not in METRIC_TYPES:
            raise ValueError('Invalid metric type: ' + typ)
        self.type: str = typ

        pb_typ = getattr(PBMetricType, typ, PBMetricType.UNTYPED)
        if pb_typ == 'gaugehistogram':
            pb_typ = PBMetricType.GAUGE_HISTOGRAM

        self.pb_mf = PBMetricFamily(
            name=name,
            help=documentation,
            type=pb_typ,
            metric=[],
            unit=unit,
        )

    @property
    def name(self) -> str:
        return self.pb_mf.name

    @property
    def documentation(self) -> str:
        return self.pb_mf.help

    @property
    def unit(self) -> str:
        return self.pb_mf.unit

    # def add_sample(self, name: str, labels: Dict[str, str], value: float, timestamp: Optional[Union[Timestamp, float]] = None, exemplar: Optional[Exemplar] = None, native_histogram: Optional[NativeHistogram] = None) -> None:
    #     """Add a sample to the metric.

    #     Internal-only, do not use."""
    #     # alesieur: what about exemplar and native_histogram? :awkward:
    #     # Counter and Histograms are the only types w/ exemplars under the hood
    #     self.pb_mf.metric.append(
    #         PBMetric(
    #             label=[PBLabelPair(name=k, value=v) for k, v in labels.items()],
    #             untyped=PBUntyped(
    #                 value=value,
    #             ),
    #             timestamp_ms=convert_timestamp_to_timestampms(timestamp),
    #         ),
    #     )
    def add_metric(self, metric: PBMetric):
        self.pb_mf.metric.append(metric)

    def extend_metric(self, metrics: Sequence[PBMetric]):
        self.pb_mv.metric.extend(metrics)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Metric) and self.type == other.type and self.pb_mf == other.pb_mf

    def __repr__(self) -> str:
        return "TODO: alesieur"  # alesieur
        # return "Metric({}, {}, {}, {}, {})".format(
        #     self.name,
        #     self.documentation,
        #     self.type,
        #     self.unit,
        #     self.samples,
        # )

    def _restricted_metric(self, names):
        """Build a snapshot of a metric with samples restricted to a given set of names."""
        metrics = [m for m in self.pb_mf.metric if m.name in names]
        if metrics:
            m = Metric(self.name, self.documentation, self.type, self.unit)
            m.pb_mf.metric.extend(metrics)
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
                timestamp=timestamp,
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
                 created: Optional[float] = None,  # alesieur
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
                created=created,
            )
        )


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
                value=value,
                timestamp=timestamp,
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
                   timestamp: Optional[Union[float, Timestamp]] = None,
                   created: Optional[float] = None,
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
                timestamp=timestamp,
                created=created,
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
                   timestamp: Optional[Union[Timestamp, float]] = None,
                   created: Optional[float] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of lists.
              Each inner list can be a pair of bucket name and value,
              or a triple of bucket name, value, and exemplar.
              The buckets must be sorted, and +Inf present.
          sum_value: The sum value of the metric.

          alesieur: proto spec says no exemplars for regular histograms
        """
        self.pb_mf.metric.append(
            make_histogram_metric(
                label_names=self._labelnames,
                label_values=labels,
                buckets=buckets,
                sum_value=sum_value,
                timestamp=timestamp,
                created=created,
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
                   created: Optional[float] = None,
                   ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of pairs of bucket names and values.
              The buckets must be sorted, and +Inf present.
          gsum_value: The sum value of the metric.
        """
        self.pb_mf.metric.append(
            make_histogram_metric(
                label_names=self._labelnames,
                label_values=labels,
                buckets=buckets,
                sum_value=gsum_value,
                timestamp=timestamp,
                created=created,
                gauge_histogram=True,
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
            PBMetric(
                label=[PBLabelPair(name=k, value=v) for k, v in labels.items()],
                untyped=PBUntyped(value=1),
                timestamp_ms=convert_timestamp_to_timestampms(timestamp),
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
                    label_names=self._labelnames + (self.name,),
                    label_values=labels + (state,),
                    value=1 if enabled else 0,
                    timestamp=timestamp,
                )
            )
