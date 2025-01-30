import os
from threading import Lock
import bisect
import math
import sys
import time
import types
from typing import (
    Any, Callable, Dict, Iterable, List, Literal, Optional, Sequence, Tuple,
    Type, TypeVar, Union,
)
import warnings

from . import values  # retain this import style for testability
from .context_managers import ExceptionCounter, InprogressTracker, Timer
from .metrics_core import Metric
from .prompb.metrics_pb2 import Exemplar as PBExemplar
from .prompb.metrics_pb2 import LabelPair as PBLabelPair
from .prompb.metrics_pb2 import Metric as PBMetric
from .prompb.utils import (
    make_counter_metric, make_gauge_metric, make_histogram_metric,
    make_native_histogram_metric, make_summary_metric, make_untyped_metric,
)
from .registry import Collector, CollectorRegistry, REGISTRY
from .samples import Exemplar
from .utils import floatToGoString, INF
from .validation import (
    _validate_exemplar, _validate_labelnames, _validate_metric_name,
)

T = TypeVar('T', bound='MetricWrapperBase')
F = TypeVar("F", bound=Callable[..., Any])

MINUTE = 60.0


def _build_full_name(metric_type, name, namespace, subsystem, unit):
    if not name:
        raise ValueError('Metric name should not be empty')
    full_name = ''
    if namespace:
        full_name += namespace + '_'
    if subsystem:
        full_name += subsystem + '_'
    full_name += name
    if metric_type == 'counter' and full_name.endswith('_total'):
        full_name = full_name[:-6]  # Munge to OpenMetrics.
    if unit and not full_name.endswith("_" + unit):
        full_name += "_" + unit
    if unit and metric_type in ('info', 'stateset'):
        raise ValueError('Metric name is of a type that cannot have a unit: ' + full_name)
    return full_name



def _get_use_created() -> bool:
    return os.environ.get("PROMETHEUS_DISABLE_CREATED_SERIES", 'False').lower() not in ('true', '1', 't')


_use_created = _get_use_created()


def disable_created_metrics():
    """Disable exporting _created metrics on counters, histograms, and summaries."""
    global _use_created
    _use_created = False


def enable_created_metrics():
    """Enable exporting _created metrics on counters, histograms, and summaries."""
    global _use_created
    _use_created = True


class MetricWrapperBase(Collector):
    _type: Optional[str] = None
    _reserved_labelnames: Sequence[str] = ()

    def _is_observable(self):
        # Whether this metric is observable, i.e.
        # * a metric without label names and values, or
        # * the child of a labelled metric.
        return not self._labelnames or (self._labelnames and self._labelvalues)

    def _raise_if_not_observable(self):
        # Functions that mutate the state of the metric, for example incrementing
        # a counter, will fail if the metric is not observable, because only if a
        # metric is observable will the value be initialized.
        if not self._is_observable():
            raise ValueError('%s metric is missing label values' % str(self._type))

    def _is_parent(self):
        return self._labelnames and not self._labelvalues

    def _get_metric(self):
        return Metric(self._name, self._documentation, self._type, self._unit)

    def describe(self) -> Iterable[Metric]:
        return [self._get_metric()]

    def collect(self) -> Iterable[Metric]:
        metric = self._get_metric()
        metric.pb_mf.metric.extend(self._samples())
        return [metric]

    def __str__(self) -> str:
        return f"{self._type}:{self._name}"

    def __repr__(self) -> str:
        metric_type = type(self)
        return f"{metric_type.__module__}.{metric_type.__name__}({self._name})"

    def __init__(self: T,
                 name: str,
                 documentation: str,
                 labelnames: Iterable[str] = (),
                 namespace: str = '',
                 subsystem: str = '',
                 unit: str = '',
                 registry: Optional[CollectorRegistry] = REGISTRY,
                 _labelvalues: Optional[Sequence[str]] = None,
                 ) -> None:
        self._name = _build_full_name(self._type, name, namespace, subsystem, unit)
        self._labelnames = _validate_labelnames(self, labelnames)
        self._labelvalues = tuple(_labelvalues or ())
        self._kwargs: Dict[str, Any] = {}
        self._documentation = documentation
        self._unit = unit

        _validate_metric_name(self._name)

        if self._is_parent():
            # Prepare the fields needed for child metrics.
            self._lock = Lock()
            self._metrics: Dict[Sequence[str], T] = {}

        if self._is_observable():
            self._metric_init()

        if not self._labelvalues:
            # Register the multi-wrapper parent metric, or if a label-less metric, the whole shebang.
            if registry:
                registry.register(self)

    def labels(self: T, *labelvalues: Any, **labelkwargs: Any) -> T:
        """Return the child for the given labelset.

        All metrics can have labels, allowing grouping of related time series.
        Taking a counter as an example:

            from prometheus_client import Counter

            c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
            c.labels('get', '/').inc()
            c.labels('post', '/submit').inc()

        Labels can also be provided as keyword arguments:

            from prometheus_client import Counter

            c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
            c.labels(method='get', endpoint='/').inc()
            c.labels(method='post', endpoint='/submit').inc()

        See the best practices on [naming](http://prometheus.io/docs/practices/naming/)
        and [labels](http://prometheus.io/docs/practices/instrumentation/#use-labels).
        """
        if not self._labelnames:
            raise ValueError('No label names were set when constructing %s' % self)

        if self._labelvalues:
            raise ValueError('{} already has labels set ({}); can not chain calls to .labels()'.format(
                self,
                dict(zip(self._labelnames, self._labelvalues))
            ))

        if labelvalues and labelkwargs:
            raise ValueError("Can't pass both *args and **kwargs")

        if labelkwargs:
            if sorted(labelkwargs) != sorted(self._labelnames):
                raise ValueError('Incorrect label names')
            labelvalues = tuple(str(labelkwargs[l]) for l in self._labelnames)
        else:
            if len(labelvalues) != len(self._labelnames):
                raise ValueError('Incorrect label count')
            labelvalues = tuple(str(l) for l in labelvalues)
        with self._lock:
            if labelvalues not in self._metrics:
                self._metrics[labelvalues] = self.__class__(
                    self._name,
                    documentation=self._documentation,
                    labelnames=self._labelnames,
                    unit=self._unit,
                    _labelvalues=labelvalues,
                    **self._kwargs
                )
            return self._metrics[labelvalues]

    def remove(self, *labelvalues: Any) -> None:
        if 'prometheus_multiproc_dir' in os.environ or 'PROMETHEUS_MULTIPROC_DIR' in os.environ:
            warnings.warn(
                "Removal of labels has not been implemented in multi-process mode yet.",
                UserWarning)

        if not self._labelnames:
            raise ValueError('No label names were set when constructing %s' % self)

        """Remove the given labelset from the metric."""
        if len(labelvalues) != len(self._labelnames):
            raise ValueError('Incorrect label count (expected %d, got %s)' % (len(self._labelnames), labelvalues))
        labelvalues = tuple(str(l) for l in labelvalues)
        with self._lock:
            if labelvalues in self._metrics:
                del self._metrics[labelvalues]

    def clear(self) -> None:
        """Remove all labelsets from the metric"""
        if 'prometheus_multiproc_dir' in os.environ or 'PROMETHEUS_MULTIPROC_DIR' in os.environ:
            warnings.warn(
                "Clearing labels has not been implemented in multi-process mode yet",
                UserWarning)
        with self._lock:
            self._metrics = {}

    def _samples(self) -> Iterable[PBMetric]:
        if self._is_parent():
            return self._multi_samples()
        else:
            return self._child_samples()

    def _multi_samples(self) -> Iterable[PBMetric]:
        with self._lock:
            metrics = self._metrics.copy()
        for labels, metric in metrics.items():
            series_labels = [PBLabelPair(name=name, value=value) for name, value in zip(self._labelnames, labels)]
            for pb_metric in metric._samples():
                new_metric = PBMetric(label=series_labels)
                new_metric.MergeFrom(pb_metric)
                yield new_metric

    def _child_samples(self) -> Iterable[PBMetric]:  # pragma: no cover
        raise NotImplementedError('_child_samples() must be implemented by %r' % self)

    def _metric_init(self):  # pragma: no cover
        """
        Initialize the metric object as a child, i.e. when it has labels (if any) set.

        This is factored as a separate function to allow for deferred initialization.
        """
        raise NotImplementedError('_metric_init() must be implemented by %r' % self)


class Counter(MetricWrapperBase):
    """A Counter tracks counts of events or running totals.

    Example use cases for Counters:
    - Number of requests processed
    - Number of items that were inserted into a queue
    - Total amount of data that a system has processed

    Counters can only go up (and be reset when the process restarts). If your use case can go down,
    you should use a Gauge instead.

    An example for a Counter:

        from prometheus_client import Counter

        c = Counter('my_failures_total', 'Description of counter')
        c.inc()     # Increment by 1
        c.inc(1.6)  # Increment by given value

    There are utilities to count exceptions raised:

        @c.count_exceptions()
        def f():
            pass

        with c.count_exceptions():
            pass

        # Count only one type of exception
        with c.count_exceptions(ValueError):
            pass

    You can also reset the counter to zero in case your logical "process" restarts
    without restarting the actual python process.

       c.reset()

    """
    _type = 'counter'

    def _metric_init(self) -> None:
        self._value = values.ValueClass(self._type, self._name, self._name + '_total', self._labelnames,
                                        self._labelvalues, self._documentation)
        self._created = time.time()

    def inc(self, amount: float = 1, exemplar: Optional[Dict[str, str]] = None) -> None:
        """Increment counter by the given amount."""
        self._raise_if_not_observable()
        if amount < 0:
            raise ValueError('Counters can only be incremented by non-negative amounts.')
        self._value.inc(amount)
        if exemplar:
            _validate_exemplar(exemplar)
            self._value.set_exemplar(Exemplar(exemplar, amount, time.time()))

    def reset(self) -> None:
        """Reset the counter to zero. Use this when a logical process restarts without restarting the actual python process."""
        self._value.set(0)
        self._created = time.time()

    def count_exceptions(self, exception: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception) -> ExceptionCounter:
        """Count exceptions in a block of code or function.

        Can be used as a function decorator or context manager.
        Increments the counter when an exception of the given
        type is raised up out of the code.
        """
        self._raise_if_not_observable()
        return ExceptionCounter(self, exception)

    def _child_samples(self) -> Iterable[PBMetric]:
        return (
            make_counter_metric(
                label_names=(),
                label_values=(),
                value=self._value.get(),
                exemplar=self._value.get_exemplar(),
                created=self._created if _use_created else None,
            ),
        )


class Gauge(MetricWrapperBase):
    """Gauge metric, to report instantaneous values.

     Examples of Gauges include:
        - Inprogress requests
        - Number of items in a queue
        - Free memory
        - Total memory
        - Temperature

     Gauges can go both up and down.

        from prometheus_client import Gauge

        g = Gauge('my_inprogress_requests', 'Description of gauge')
        g.inc()      # Increment by 1
        g.dec(10)    # Decrement by given value
        g.set(4.2)   # Set to a given value

     There are utilities for common use cases:

        g.set_to_current_time()   # Set to current unixtime

        # Increment when entered, decrement when exited.
        @g.track_inprogress()
        def f():
            pass

        with g.track_inprogress():
            pass

     A Gauge can also take its value from a callback:

        d = Gauge('data_objects', 'Number of objects')
        my_dict = {}
        d.set_function(lambda: len(my_dict))
    """
    _type = 'gauge'
    _MULTIPROC_MODES = frozenset(('all', 'liveall', 'min', 'livemin', 'max', 'livemax', 'sum', 'livesum', 'mostrecent', 'livemostrecent'))
    _MOST_RECENT_MODES = frozenset(('mostrecent', 'livemostrecent'))

    def __init__(self,
                 name: str,
                 documentation: str,
                 labelnames: Iterable[str] = (),
                 namespace: str = '',
                 subsystem: str = '',
                 unit: str = '',
                 registry: Optional[CollectorRegistry] = REGISTRY,
                 _labelvalues: Optional[Sequence[str]] = None,
                 multiprocess_mode: Literal['all', 'liveall', 'min', 'livemin', 'max', 'livemax', 'sum', 'livesum', 'mostrecent', 'livemostrecent'] = 'all',
                 ):
        self._multiprocess_mode = multiprocess_mode
        if multiprocess_mode not in self._MULTIPROC_MODES:
            raise ValueError('Invalid multiprocess mode: ' + multiprocess_mode)
        super().__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            namespace=namespace,
            subsystem=subsystem,
            unit=unit,
            registry=registry,
            _labelvalues=_labelvalues,
        )
        self._kwargs['multiprocess_mode'] = self._multiprocess_mode
        self._is_most_recent = self._multiprocess_mode in self._MOST_RECENT_MODES

    def _metric_init(self) -> None:
        self._value = values.ValueClass(
            self._type, self._name, self._name, self._labelnames, self._labelvalues,
            self._documentation, multiprocess_mode=self._multiprocess_mode
        )

    def inc(self, amount: float = 1) -> None:
        """Increment gauge by the given amount."""
        if self._is_most_recent:
            raise RuntimeError("inc must not be used with the mostrecent mode")
        self._raise_if_not_observable()
        self._value.inc(amount)

    def dec(self, amount: float = 1) -> None:
        """Decrement gauge by the given amount."""
        if self._is_most_recent:
            raise RuntimeError("dec must not be used with the mostrecent mode")
        self._raise_if_not_observable()
        self._value.inc(-amount)

    def set(self, value: float) -> None:
        """Set gauge to the given value."""
        self._raise_if_not_observable()
        if self._is_most_recent:
            self._value.set(float(value), timestamp=time.time())
        else:
            self._value.set(float(value))

    def set_to_current_time(self) -> None:
        """Set gauge to the current unixtime."""
        self.set(time.time())

    def track_inprogress(self) -> InprogressTracker:
        """Track inprogress blocks of code or functions.

        Can be used as a function decorator or context manager.
        Increments the gauge when the code is entered,
        and decrements when it is exited.
        """
        self._raise_if_not_observable()
        return InprogressTracker(self)

    def time(self) -> Timer:
        """Time a block of code or function, and set the duration in seconds.

        Can be used as a function decorator or context manager.
        """
        return Timer(self, 'set')

    def set_function(self, f: Callable[[], float]) -> None:
        """Call the provided function to return the Gauge value.

        The function must return a float, and may be called from
        multiple threads. All other methods of the Gauge become NOOPs.
        """

        self._raise_if_not_observable()

        def samples(_: Gauge) -> Iterable[PBMetric]:
            return (make_gauge_metric(label_names=(), label_values=(), value=float(f())),)

        self._child_samples = types.MethodType(samples, self)  # type: ignore

    def _child_samples(self) -> Iterable[PBMetric]:
        return (make_gauge_metric(label_names=(), label_values=(), value=self._value.get()),)


class Summary(MetricWrapperBase):
    """A Summary tracks the size and number of events.

    Example use cases for Summaries:
    - Response latency
    - Request size

    Example for a Summary:

        from prometheus_client import Summary

        s = Summary('request_size_bytes', 'Request size (bytes)')
        s.observe(512)  # Observe 512 (bytes)

    Example for a Summary using time:

        from prometheus_client import Summary

        REQUEST_TIME = Summary('response_latency_seconds', 'Response latency (seconds)')

        @REQUEST_TIME.time()
        def create_response(request):
          '''A dummy function'''
          time.sleep(1)

    Example for using the same Summary object as a context manager:

        with REQUEST_TIME.time():
            pass  # Logic to be timed
    """
    _type = 'summary'
    _reserved_labelnames = ['quantile']

    def _metric_init(self) -> None:
        self._count = values.ValueClass(self._type, self._name, self._name + '_count', self._labelnames,
                                        self._labelvalues, self._documentation)
        self._sum = values.ValueClass(self._type, self._name, self._name + '_sum', self._labelnames, self._labelvalues, self._documentation)
        self._created = time.time()

    def observe(self, amount: float) -> None:
        """Observe the given amount.

        The amount is usually positive or zero. Negative values are
        accepted but prevent current versions of Prometheus from
        properly detecting counter resets in the sum of
        observations. See
        https://prometheus.io/docs/practices/histograms/#count-and-sum-of-observations
        for details.
        """
        self._raise_if_not_observable()
        self._count.inc(1)
        self._sum.inc(amount)

    def time(self) -> Timer:
        """Time a block of code or function, and observe the duration in seconds.

        Can be used as a function decorator or context manager.
        """
        return Timer(self, 'observe')

    def _child_samples(self) -> Iterable[PBMetric]:
        return (
            make_summary_metric(
                label_names=(),
                label_values=(),
                sample_count=self._count.get(),
                sample_sum=self._sum.get(),
                created=self._created if _use_created else None,
            ),
        )


# native_histogram_bounds = [[] for _ in range(9)]
# num_buckets = 1
# for i in range(len(native_histogram_bounds)):
#     bounds = [0.5]
#     factor = 2 ** (2 ** -i)

#     for j in range(num_buckets - 1):
#         if (j+1) % 2 == 0:
#             bound = native_histogram_bounds[i-1][j//2 + 1]
#         else:
#             bound = bounds[j] * factor
#         bounds.append(bound)
#     num_buckets *= 2
#     native_histogram_bounds[i] = bounds


# mirroring what client_golang does for now
class HistogramCounts:
    def __init__(self, bucket_count: int, zero_threshold: float, schema: int):
        self.sum = values.ValueClass()
        self.count = values.ValueClass()

        self.nh_zero_bucket = values.ValueClass()
        self.nh_zero_threshold = values.ValueClass(zero_threshold)
        self.nh_schema = values.ValueClass(schema)
        self.nh_buckets_number = values.ValueClass()
        self.buckets = [values.ValueClass() for _ in range(bucket_count)]
        self.nh_buckets_positive: dict[int, int] = {}
        self.nh_buckets_positive_lock = Lock()
        self.nh_buckets_negative: dict[int, int] = {}
        self.nh_buckets_negative_lock = Lock()

    def inc_bucket(self, sign: int, key: int) -> bool:
        if sign == 0:
            self.nh_zero_bucket.inc()
            return False
        
        if sign < 0:
            lock = self.nh_buckets_negative_lock
            buckets = self.nh_buckets_negative
        else:
            lock = self.nh_buckets_positive_lock
            buckets = self.nh_buckets_positive

        with lock:
            if key in buckets:
                buckets[key] += 1
                return False
            else:
                buckets[key] = 1
                return True

    def observe(self, v: float, bucket: int, do_sparse: bool, exemplar: Optional[Dict[str, str]] = None):
        if bucket < len(self.buckets):
            self.buckets[bucket].inc()
            if exemplar:
                self.buckets[bucket].set_exemplar(Exemplar(exemplar, v, time.time()))

        self.sum.inc(v)

        # if doSparse && !math.IsNaN(v) {
        if do_sparse:
            zero_threshold = self.nh_zero_threshold.get()
            schema = self.nh_schema.get()
            is_inf = False
            key = 0
            bucket_created = False
            
            if math.isinf(v):
                v = math.copysign(sys.float_info.max, v)
                is_inf = True

            frac, exp = math.frexp(abs(v))

            if schema > 0:
                bounds = self.nh_bounds[schema]
                key = bisect.bisect_left(bounds, frac) + (exp-1)*len(bounds)
            else:
                key = exp
                if frac == 0.5:
                    key -= 1
                offset = (1 << -schema) - 1
                key = (key + offset) >> -schema
                if is_inf:
                    key += 1
                bucket_created = self.inc_bucket(0 if abs(v) <= zero_threshold else math.copysign(1, v), key)
                if bucket_created:
                    self.nh_buckets_number.inc()
        self.count.inc()



class Histogram(MetricWrapperBase):
    """A Histogram tracks the size and number of events in buckets.

    You can use Histograms for aggregatable calculation of quantiles.

    Example use cases:
    - Response latency
    - Request size

    Example for a Histogram:

        from prometheus_client import Histogram

        h = Histogram('request_size_bytes', 'Request size (bytes)')
        h.observe(512)  # Observe 512 (bytes)

    Example for a Histogram using time:

        from prometheus_client import Histogram

        REQUEST_TIME = Histogram('response_latency_seconds', 'Response latency (seconds)')

        @REQUEST_TIME.time()
        def create_response(request):
          '''A dummy function'''
          time.sleep(1)

    Example of using the same Histogram object as a context manager:

        with REQUEST_TIME.time():
            pass  # Logic to be timed

    The default buckets are intended to cover a typical web/rpc request from milliseconds to seconds.
    They can be overridden by passing `buckets` keyword argument to `Histogram`.
    """
    _type = 'histogram'
    _reserved_labelnames = ['le']
    DEFAULT_BUCKETS = (.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, INF)

    NATIVE_HISTOGRAM_SCHEMA_MINIMUM = -4
    NATIVE_HISTOGRAM_SCHEMA_MAXIMUM = 8

    NATIVE_HISTOGRAM_BOUNDS = [
        # schema 0
        [0.5],
        # schema 1
        [0.5, 0.7071067811865476],
        # schema 2
        [0.5, 0.5946035575013605, 0.7071067811865476, 0.8408964152537146],
        # schema 3
        [
            0.5, 0.5452538663326288, 0.5946035575013605, 0.6484197773255048,
            0.7071067811865476, 0.7711054127039705, 0.8408964152537146, 0.9170040432046713,
        ],
        # schema 4
        [
            0.5, 0.5221368912137069, 0.5452538663326288, 0.5693943173783458,
            0.5946035575013605, 0.620928906036742, 0.6484197773255048, 0.6771277734684463,
            0.7071067811865476, 0.7384130729697497, 0.7711054127039705, 0.8052451659746271,
            0.8408964152537146, 0.8781260801866497, 0.9170040432046713, 0.9576032806985737,
        ],
        # schema 5
        [
            0.5, 0.5109485743270583, 0.5221368912137069, 0.5335702003384117,
            0.5452538663326288, 0.5571933712979462, 0.5693943173783458, 0.5818624293887887,
            0.5946035575013605, 0.6076236799902344, 0.620928906036742, 0.6345254785958666,
            0.6484197773255048, 0.6626183215798706, 0.6771277734684463, 0.6919549409819159,
            0.7071067811865476, 0.7225904034885233, 0.7384130729697497, 0.7545822137967113,
            0.7711054127039705, 0.7879904225539432, 0.8052451659746271, 0.8228777390769824,
            0.8408964152537146, 0.859309649061239, 0.8781260801866497, 0.8973545375015536,
            0.9170040432046713, 0.93708381705515, 0.9576032806985737, 0.9785720620877001,
        ],
        # schema 6
        [
            0.5, 0.5054446430258502, 0.5109485743270583, 0.5165124395106142,
            0.5221368912137069, 0.5278225891802786, 0.5335702003384117, 0.5393803988785598,
            0.5452538663326288, 0.5511912916539204, 0.5571933712979462, 0.5632608093041209,
            0.5693943173783458, 0.5755946149764913, 0.5818624293887887, 0.5881984958251406,
            0.5946035575013605, 0.6010783657263515, 0.6076236799902344, 0.6142402680534349,
            0.620928906036742, 0.6276903785123455, 0.6345254785958666, 0.6414350080393891,
            0.6484197773255048, 0.6554806057623822, 0.6626183215798706, 0.6698337620266515,
            0.6771277734684463, 0.6845012114872953, 0.6919549409819159, 0.6994898362691555,
            0.7071067811865476, 0.714806669195985, 0.7225904034885233, 0.7304588970903235,
            0.7384130729697497, 0.7464538641456324, 0.7545822137967113, 0.7627990753722691,
            0.7711054127039705, 0.7795022001189186, 0.7879904225539432, 0.7965710756711335,
            0.8052451659746271, 0.8140137109286739, 0.8228777390769824, 0.8318382901633682,
            0.8408964152537146, 0.8500531768592618, 0.859309649061239, 0.8686669176368531,
            0.8781260801866497, 0.8876882462632606, 0.8973545375015536, 0.9071260877501994,
            0.9170040432046713, 0.9269895625416928, 0.93708381705515, 0.9472879907934829,
            0.9576032806985737, 0.9680308967461473, 0.9785720620877001, 0.9892280131939755,
        ],
        # schema 7
        [
            0.5, 0.5027149505564014, 0.5054446430258502, 0.5081891574554764,
            0.5109485743270583, 0.5137229745593818, 0.5165124395106142, 0.5193170509806894,
            0.5221368912137069, 0.5249720429003435, 0.5278225891802786, 0.5306886136446309,
            0.5335702003384117, 0.5364674337629877, 0.5393803988785598, 0.5423091811066545,
            0.5452538663326288, 0.5482145409081883, 0.5511912916539204, 0.5541842058618393,
            0.5571933712979462, 0.5602188762048033, 0.5632608093041209, 0.5663192597993595,
            0.5693943173783458, 0.572486072215902, 0.5755946149764913, 0.5787200368168754,
            0.5818624293887887, 0.585021884841625, 0.5881984958251406, 0.5913923554921704,
            0.5946035575013605, 0.5978321960199137, 0.6010783657263515, 0.6043421618132907,
            0.6076236799902344, 0.6109230164863786, 0.6142402680534349, 0.6175755319684665,
            0.620928906036742, 0.6243004885946023, 0.6276903785123455, 0.6310986751971253,
            0.6345254785958666, 0.637970889198196, 0.6414350080393891, 0.6449179367033329,
            0.6484197773255048, 0.6519406325959679, 0.6554806057623822, 0.659039800633032,
            0.6626183215798706, 0.6662162735415805, 0.6698337620266515, 0.6734708931164728,
            0.6771277734684463, 0.6808045103191123, 0.6845012114872953, 0.688217985377265,
            0.6919549409819159, 0.6957121878859629, 0.6994898362691555, 0.7032879969095076,
            0.7071067811865476, 0.7109463010845828, 0.714806669195985, 0.7186879987244911,
            0.7225904034885233, 0.7265139979245262, 0.7304588970903235, 0.7344252166684909,
            0.7384130729697497, 0.7424225829363762, 0.7464538641456324, 0.7505070348132127,
            0.7545822137967113, 0.7586795205991073, 0.7627990753722691, 0.7669409989204778,
            0.7711054127039705, 0.7752924388425, 0.7795022001189186, 0.7837348199827765,
            0.7879904225539432, 0.7922691326262468, 0.7965710756711335, 0.8008963778413467,
            0.8052451659746271, 0.8096175675974318, 0.8140137109286739, 0.8184337248834822,
            0.8228777390769824, 0.827345883828097, 0.8318382901633682, 0.8363550898207982,
            0.8408964152537146, 0.8454623996346525, 0.8500531768592618, 0.8546688815502315,
            0.859309649061239, 0.8639756154809187, 0.8686669176368531, 0.8733836930995844,
            0.8781260801866497, 0.8828942179666364, 0.8876882462632606, 0.8925083056594674,
            0.8973545375015536, 0.9022270839033119, 0.9071260877501994, 0.9120516927035266,
            0.9170040432046713, 0.921983284479313, 0.9269895625416928, 0.9320230241988945,
            0.93708381705515, 0.9421720895161672, 0.9472879907934829, 0.9524316709088371,
            0.9576032806985737, 0.9628029718180624, 0.9680308967461473, 0.9732872087896166,
            0.9785720620877001, 0.9838856116165877, 0.9892280131939755, 0.9945994234836331,
        ],
        # schema 8
        [
            0.5, 0.5013556375251013, 0.5027149505564014, 0.5040779490592088,
            0.5054446430258502, 0.5068150424757447, 0.5081891574554764, 0.509566998038869,
            0.5109485743270583, 0.5123338964485679, 0.5137229745593818, 0.5151158188430205,
            0.5165124395106142, 0.5179128468009786, 0.5193170509806894, 0.520725062344158,
            0.5221368912137069, 0.5235525479396449, 0.5249720429003435, 0.526395386502313,
            0.5278225891802786, 0.5292536613972564, 0.5306886136446309, 0.5321274564422321,
            0.5335702003384117, 0.5350168559101208, 0.5364674337629877, 0.5379219445313954,
            0.5393803988785598, 0.5408428074966075, 0.5423091811066545, 0.5437795304588847,
            0.5452538663326288, 0.5467321995364429, 0.5482145409081883, 0.549700901315111,
            0.5511912916539204, 0.5526857228508706, 0.5541842058618393, 0.5556867516724088,
            0.5571933712979462, 0.5587040757836845, 0.5602188762048033, 0.5617377836665098,
            0.5632608093041209, 0.564787964283144, 0.5663192597993595, 0.5678547070789026,
            0.5693943173783458, 0.5709381019847808, 0.572486072215902, 0.5740382394200894,
            0.5755946149764913, 0.5771552102951081, 0.5787200368168754, 0.5802891060137493,
            0.5818624293887887, 0.5834400184762408, 0.585021884841625, 0.5866080400818185,
            0.5881984958251406, 0.5897932637314379, 0.5913923554921704, 0.5929957828304968,
            0.5946035575013605, 0.5962156912915756, 0.5978321960199137, 0.5994530835371903,
            0.6010783657263515, 0.6027080545025619, 0.6043421618132907, 0.6059806996384005,
            0.6076236799902344, 0.6092711149137041, 0.6109230164863786, 0.6125793968185725,
            0.6142402680534349, 0.6159056423670379, 0.6175755319684665, 0.6192499490999082,
            0.620928906036742, 0.622612415087629, 0.6243004885946023, 0.6259931389331581,
            0.6276903785123455, 0.6293922197748583, 0.6310986751971253, 0.6328097572894031,
            0.6345254785958666, 0.6362458516947014, 0.637970889198196, 0.6397006037528346,
            0.6414350080393891, 0.6431741147730128, 0.6449179367033329, 0.6466664866145447,
            0.6484197773255048, 0.6501778216898253, 0.6519406325959679, 0.6537082229673385,
            0.6554806057623822, 0.6572577939746774, 0.659039800633032, 0.6608266388015788,
            0.6626183215798706, 0.6644148621029772, 0.6662162735415805, 0.6680225691020727,
            0.6698337620266515, 0.6716498655934177, 0.6734708931164728, 0.6752968579460171,
            0.6771277734684463, 0.6789636531064505, 0.6808045103191123, 0.6826503586020058,
            0.6845012114872953, 0.6863570825438342, 0.688217985377265, 0.690083933630119,
            0.6919549409819159, 0.6938310211492645, 0.6957121878859629, 0.6975984549830999,
            0.6994898362691555, 0.7013863456101023, 0.7032879969095076, 0.7051948041086352,
            0.7071067811865476, 0.7090239421602077, 0.7109463010845828, 0.7128738720527472,
            0.714806669195985, 0.7167447066838945, 0.7186879987244911, 0.7206365595643127,
            0.7225904034885233, 0.7245495448210175, 0.7265139979245262, 0.7284837772007219,
            0.7304588970903235, 0.732439372073203, 0.7344252166684909, 0.7364164454346838,
            0.7384130729697497, 0.7404151139112359, 0.7424225829363762, 0.7444354947621985,
            0.7464538641456324, 0.7484777058836177, 0.7505070348132127, 0.7525418658117032,
            0.7545822137967113, 0.7566280937263049, 0.7586795205991073, 0.7607365094544072,
            0.7627990753722691, 0.7648672334736435, 0.7669409989204778, 0.7690203869158283,
            0.7711054127039705, 0.7731960915705108, 0.7752924388425, 0.7773944698885443,
            0.7795022001189186, 0.7816156449856789, 0.7837348199827765, 0.7858597406461708,
            0.7879904225539432, 0.7901268813264123, 0.7922691326262468, 0.7944171921585819,
            0.7965710756711335, 0.7987307989543136, 0.8008963778413467, 0.8030678282083855,
            0.8052451659746271, 0.8074284071024304, 0.8096175675974318, 0.8118126635086643,
            0.8140137109286739, 0.8162207259936376, 0.8184337248834822, 0.8206527238220032,
            0.8228777390769824, 0.8251087869603089, 0.827345883828097, 0.829589046080808,
            0.8318382901633682, 0.8340936325652912, 0.8363550898207982, 0.8386226785089392,
            0.8408964152537146, 0.8431763167241968, 0.8454623996346525, 0.8477546807446663,
            0.8500531768592618, 0.8523579048290257, 0.8546688815502315, 0.8569861239649631,
            0.859309649061239, 0.861639473873137, 0.8639756154809187, 0.8663180910111555,
            0.8686669176368531, 0.8710221125775782, 0.8733836930995844, 0.8757516765159391,
            0.8781260801866497, 0.880506921518792, 0.8828942179666364, 0.8852879870317774,
            0.8876882462632606, 0.8900950132577122, 0.8925083056594674, 0.8949281411607004,
            0.8973545375015536, 0.8997875124702676, 0.9022270839033119, 0.9046732696855159,
            0.9071260877501994, 0.9095855560793044, 0.9120516927035266, 0.9145245157024486,
            0.9170040432046713, 0.919490293387947, 0.921983284479313, 0.9244830347552255,
            0.9269895625416928, 0.9295028862144102, 0.9320230241988945, 0.9345499949706193,
            0.93708381705515, 0.9396245090282802, 0.9421720895161672, 0.9447265771954695,
            0.9472879907934829, 0.9498563490882778, 0.9524316709088371, 0.9550139751351949,
            0.9576032806985737, 0.9601996065815238, 0.9628029718180624, 0.9654133954938136,
            0.9680308967461473, 0.9706554947643203, 0.9732872087896166, 0.9759260581154892,
            0.9785720620877001, 0.9812252401044637, 0.9838856116165877, 0.986553196127617,
            0.9892280131939755, 0.9919100824251097, 0.9945994234836331, 0.99729605608547,
        ],
    ]

    # DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD is the default value for
    # NATIVE_HISTOGRAM_ZERO_THRESHOLD in the HISTOGRAM_OPTS.
    #
    # The value is 2^-128 (or 0.5*2^-127 in the actual IEEE 754 representation),
    # which is a bucket boundary at all possible resolutions.
    DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD = 2.938735877055719e-39

    # NATIVE_HISTOGRAM_ZERO_THRESHOLD_ZERO can be used as NATIVE_HISTOGRAM_ZERO_THRESHOLD
    # in the HISTOGRAM_OPTS to create a zero bucket of width zero, i.e. a zero
    # bucket that only receives observations of precisely zero.
    NATIVE_HISTOGRAM_ZERO_THRESHOLD_ZERO = -1

    def __init__(self,
                 name: str,
                 documentation: str,
                 labelnames: Iterable[str] = (),
                 namespace: str = '',
                 subsystem: str = '',
                 unit: str = '',
                 registry: Optional[CollectorRegistry] = REGISTRY,
                 _labelvalues: Optional[Sequence[str]] = None,
                 buckets: Optional[Sequence[Union[float, str]]] = None,
                 # If nh_bucket_factor is greater than one, so-called sparse
                 # buckets are used (in addition to the regular buckets, if defined
                 # above). A Histogram with sparse buckets will be ingested as a Native
                 # Histogram by a Prometheus server with that feature enabled (requires
                 # Prometheus v2.40+). Sparse buckets are exponential buckets covering
                 # the whole float64 range (with the exception of the “zero” bucket, see
                 # nh_zero_threshold below). From any one bucket to the next,
                 # the width of the bucket grows by a constant
                 # factor. nh_bucket_factor provides an upper bound for this
                 # factor (exception see below). The smaller
                 # nh_bucket_factor, the more buckets will be used and thus
                 # the more costly the histogram will become. A generally good trade-off
                 # between cost and accuracy is a value of 1.1 (each bucket is at most
                 # 10% wider than the previous one), which will result in each power of
                 # two divided into 8 buckets (e.g. there will be 8 buckets between 1
                 # and 2, same as between 2 and 4, and 4 and 8, etc.).
                 #
                 # Details about the actually used factor: The factor is calculated as
                 # 2^(2^-n), where n is an integer number between (and including) -4 and
                 # 8. n is chosen so that the resulting factor is the largest that is
                 # still smaller or equal to nh_bucket_factor. Note that the
                 # smallest possible factor is therefore approx. 1.00271 (i.e. 2^(2^-8)
                 # ). If nh_bucket_factor is greater than 1 but smaller than
                 # 2^(2^-8), then the actually used factor is still 2^(2^-8) even though
                 # it is larger than the provided nh_bucket_factor.
                 nh_bucket_factor: float = 0.0,
                 # All observations with an absolute value of less or equal
                 # nh_zero_threshold are accumulated into a “zero” bucket.
                 # For best results, this should be close to a bucket boundary. This is
                 # usually the case if picking a power of two. 
                 # To configure a zero bucket with an actual threshold of zero (i.e. only
                 # observations of precisely zero will go into the zero bucket), set
                 # nh_zero_threshold to the nh_ZERO_THRESHOLD_ZERO
                 # constant (or any negative float value).
                 nh_zero_threshold: Optional[float] = None,
                 # The next three fields define a strategy to limit the number of
                 # populated sparse buckets. If nh_max_bucket_number is left
                 # at zero, the number of buckets is not limited. (Note that this might
                 # lead to unbounded memory consumption if the values observed by the
                 # Histogram are sufficiently wide-spread. In particular, this could be
                 # used as a DoS attack vector. Where the observed values depend on
                 # external inputs, it is highly recommended to set a
                 # nh_max_bucket_number.) Once the set
                 # nh_max_bucket_number is exceeded, the following strategy is
                 # enacted:
                 #  - First, if the last reset (or the creation) of the histogram is at
                 #    least nh_min_reset_duration ago, then the whole
                 #    histogram is reset to its initial state (including regular
                 #    buckets).
                 #  - If less time has passed, or if nh_min_reset_duration is
                 #    zero, no reset is performed. Instead, the zero threshold is
                 #    increased sufficiently to reduce the number of buckets to or below
                 #    nh_max_bucket_number, but not to more than
                 #    nh_max_zero_threshold. Thus, if
                 #    nh_max_zero_threshold is already at or below the current
                 #    zero threshold, nothing happens at this step.
                 #  - After that, if the number of buckets still exceeds
                 #    nh_max_bucket_number, the resolution of the histogram is
                 #    reduced by doubling the width of the sparse buckets (up to a
                 #    growth factor between one bucket to the next of 2^(2^4) = 65536,
                 #    see above).
                 #  - Any increased zero threshold or reduced resolution is reset back
                 #    to their original values once nh_min_reset_duration has
                 #    passed (since the last reset or the creation of the histogram).
                 nh_max_bucket_number: int = 0,
                 nh_min_reset_duration: Optional[float] = None,
                 nh_max_zero_threshold: float = 0.0,
                 # nh_max_exemplars limits the number of exemplars
                 # that are kept in memory for each native histogram. If you leave it at
                 # zero, a default value of 10 is used. If no exemplars should be kept specifically
                 # for native histograms, set it to a negative value. (Scrapers can
                 # still use the exemplars exposed for classic buckets, which are managed
                 # independently.)
                 nh_max_exemplars: int = 10,
                 # nh_exemplar_ttl is only checked once
                 # nh_max_exemplars is exceeded. In that case, the
                 # oldest exemplar is removed if it is older than nh_exemplar_ttl.
                 # Otherwise, the older exemplar in the pair of exemplars that are closest
                 # together (on an exponential scale) is removed.
                 # If nh_exemplar_ttl is left at its zero value, a default value of
                 # 5m is used. To always delete the oldest exemplar, set it to a negative value.
                 nh_exemplar_ttl: Optional[float] = None,
                 ):
        self._prepare_buckets(
            source_buckets=buckets,
            nh_bucket_factor=nh_bucket_factor,
            nh_zero_threshold=nh_zero_threshold,
            nh_max_exemplars=nh_max_exemplars,
            nh_exemplar_ttl=nh_exemplar_ttl,
            name=name,
            labelnames=labelnames,
            labelvalues=_labelvalues,
            documentation=documentation,
        )
        super().__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            namespace=namespace,
            subsystem=subsystem,
            unit=unit,
            registry=registry,
            _labelvalues=_labelvalues,
        )
        self._kwargs['buckets'] = buckets
        self._kwargs['nh_bucket_factor'] = nh_bucket_factor
        self._kwargs['nh_zero_threshold'] = nh_zero_threshold
        self._kwargs['nh_max_bucket_number'] = nh_max_bucket_number
        self._kwargs['nh_min_reset_duration'] = nh_min_reset_duration
        self._kwargs['nh_max_zero_threshold'] = nh_max_zero_threshold
        self._kwargs['nh_max_exemplars'] = nh_max_exemplars
        self._kwargs['nh_exemplar_ttl'] = nh_exemplar_ttl

        self.count_and_hot_idx = values.ValueClass()
        self.mtx = Lock()
        self.label_pairs = [PBLabelPair(name=name, value=value) for name, value in zip(self._labelnames, self._labelvalues)]
        # alesieur: golang stores exemplars separately
        # python attempts to do it in the "atomic" value, but we don't do it in multiprocess
        # will need to figure out how to do this right

        # self.nh_schema is set in _prepare_buckets
        # self.nh_zero_threshold is set in _prepare_buckets
        self.nh_max_zero_threshold = nh_max_zero_threshold
        self.nh_max_bucket_number = nh_max_bucket_number
        self.nh_min_reset_duration = nh_min_reset_duration
        self.last_reset_time: Optional[float] = None
        self.reset_scheduled = False
        self.now = time.time
        # self.now for testing purposes
        # self.afterFunc for testing purposes

    # pick_schema returns the largest number n between -4 and 8 such that
    # 2^(2^-n) is less or equal the provided bucket_factor.
    #
    # Special cases:
    #   - bucket_factor <= 1: panics.
    #   - bucket_factor < 2^(2^-8) (but > 1): still returns 8.
    def pick_schema(self, bucket_factor: float) -> int:
        if bucket_factor <= 1:
            raise ValueError(f"bucket_factor {bucket_factor} is <=1")
        floor = math.floor(math.log2(math.log2(bucket_factor)))
        if floor <= -8:
            return self.NATIVE_HISTOGRAM_SCHEMA_MAXIMUM
        if floor >= 4:
            return self.NATIVE_HISTOGRAM_SCHEMA_MINIMUM
        return -int(floor)

    def _prepare_native_exemplars(
        self,
        ttl: Optional[float],
        max_count: Optional[int],
    ):
        if not ttl:
            ttl = 5 * MINUTE
        if not max_count:
            max_count = 10
        if max_count < 0:
            max_count = 0
            ttl = None

        self.nh_exemplars_max_count = max_count
        self.nh_exemplars_ttl = ttl

    # alesieur: might need to handle +Inf for native histograms?
    def _prepare_buckets(
        self,
        source_buckets: Sequence[Union[float, str]],
        nh_bucket_factor: float,
        nh_zero_threshold: float,
        nh_max_exemplars: int,
        nh_exemplar_ttl: float,
        name: str,
        labelnames: Iterable[str],
        labelvalues: Optional[Sequence[str]],
        documentation: str,
    ) -> None:
        if source_buckets is None and nh_bucket_factor <= 1:
            source_buckets = self.DEFAULT_BUCKETS
        if nh_bucket_factor <= 1:
            self.nh_schema = None  # to mark there are no sparse buckets self.NATIVE_HISTOGRAM_SCHEMA_MINIMUM - 1
        else:
            if nh_zero_threshold > 0:
                self.nh_zero_threshold = nh_zero_threshold
            elif nh_zero_threshold is None:
                self.nh_zero_threshold = self.DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD
            else:
                self.nh_zero_threshold = 0.0
            self.nh_schema = self.pick_chema(nh_bucket_factor)
            self._prepare_native_exemplars(nh_exemplar_ttl, nh_max_exemplars)

        buckets = [float(b) for b in source_buckets]
        if buckets != sorted(buckets):
            # This is probably an error on the part of the user,
            # so raise rather than sorting for them.
            raise ValueError('Buckets not in sorted order')
        if buckets and buckets[-1] != INF:
            buckets.append(INF)
        if len(buckets) < 2:
            raise ValueError('Must have at least two buckets')
        self._upper_bounds = buckets

        self._counts = [
            HistogramCounts(len(self._upper_bounds)),
            HistogramCounts(len(self._upper_bounds)),
        ]
        self._counts[0].nh_zero_threshold.set(self.nh_zero_threshold)
        self._counts[0].nh_schema.set(self.nh_schema)
        self._counts[1].nh_zero_threshold.set(self.nh_zero_threshold)
        self._counts[1].nh_schema.set(self.nh_schema)

    def _metric_init(self) -> None:
        self._buckets: List[values.ValueClass] = []
        self._created = time.time()
        bucket_labelnames = self._labelnames + ('le',)
        self._sum = values.ValueClass(self._type, self._name, self._name + '_sum', self._labelnames, self._labelvalues, self._documentation)
        for b in self._upper_bounds:
            self._buckets.append(values.ValueClass(
                self._type,
                self._name,
                self._name + '_bucket',
                bucket_labelnames,
                self._labelvalues + (floatToGoString(b),),
                self._documentation)
            )

    def find_bucket(self, v: float) -> int:
        n = len(self._upper_bounds)
        if n == 0:
            return 0

        if v <= self._upper_bounds[0]:
            return 0

        if v > self._upper_bounds[-1]:
            return n

        # alesieur
        # golang folks did some testing that showed that for arrays shorter than 35 elements
        # linear search was faster, for everything else (outside of the edge cases above) bisect
        # was faster. I'm going to assume these results are good enough for python for now. We
        # can do actual testing later.
        # https://github.com/prometheus/client_golang/pull/1662
        if n < 35:
            return next((i for i, bound in enumerate(self._upper_bounds) if v <= bound), n)

        return bisect.bisect_left(self._upper_bounds, v)

    def observe(self, amount: float, exemplar: Optional[Dict[str, str]] = None) -> None:
        """Observe the given amount.

        The amount is usually positive or zero. Negative values are
        accepted but prevent current versions of Prometheus from
        properly detecting counter resets in the sum of
        observations. See
        https://prometheus.io/docs/practices/histograms/#count-and-sum-of-observations
        for details.
        """
        self._raise_if_not_observable()

        do_sparse = self.nh_schema is not None

        n = self.count_and_hot_idx.get() + 1
        hot_counts = self.counts[n>>63]
        bucket = self.find_bucket(amount)
        hot_counts.observe(amount, bucket, do_sparse, exemplar)

        if do_sparse:
            self.limit_buckets(hot_counts, amount, bucket)

        # self._sum.inc(amount)
        # for i, bound in enumerate(self._upper_bounds):
        #     if amount <= bound:
        #         self._buckets[i].inc(1)
        #         if exemplar:
        #             _validate_exemplar(exemplar)
        #             self._buckets[i].set_exemplar(Exemplar(exemplar, amount, time.time()))
        #         break

    def udpate_exemplar(self, v: float, bucket: int, labels: Dict[str, str]) -> None:
        # alesieur
        # golang does some label enforcement things here, skipping for now
        e = PBExemplar(
            label=[PBLabelPair(name=name, value=value) for name, value in labels.items()],
            value=v,
            timestamp=self.now(),
        )
        # alesieur: need a lock
        self._exemplars[bucket] = e
        if self.nh_schema:
            self._native_exemplars.append(e)


    def time(self) -> Timer:
        """Time a block of code or function, and observe the duration in seconds.

        Can be used as a function decorator or context manager.
        """
        return Timer(self, 'observe')

    def _wait_for_cooldown(self, count: int, counts: HistogramCounts):
        while count != counts.count.get():
            time.sleep(0)  # apparently that's the equivalent to runtime.Gosched ?


    def _child_samples(self) -> Iterable[PBMetric]:
        with self.mtx:
            # alesieur
            # just copying what happens in golang and hope it works :shrug:
            n = self.count_and_hot_idx.get() + 1<<63
            count = n & ((1 << 63) -1)
            hot_counts = self.counts[n>>63]
            cold_counts = self.counts[((1 << 64) - 1 - n)>>63]

            self._wait_for_cooldown(count, cold_counts)

            buckets = []
            acc = 0.0
            for i, bound in enumerate(self._upper_bounds):
                acc += self._buckets[i].get()
                buckets.append((str(bound), acc, self._buckets[i].get_exemplar()))

            # return (
            #     (
            #         make_histogram_metric(
            #             label_names=(),
            #             label_values=(),
            #             buckets=buckets,
            #             sum_value=self._sum.get(),
            #             created=self._created if _use_created else None,
            #         )
            #     ),
            # )

            if self.nh_schema is not None:
                metric = make_native_histogram_metric(
                    label_names=(),
                    label_values=(),
                    buckets=buckets,
                    cold_counts=cold_counts,
                    created=self.last_reset_time,
                )
            else:
                metric = make_histogram_metric(
                    label_names=(),
                    label_values=(),
                    buckets=buckets,
                    sum_value=self._sum.get(),
                    created=self._created if _use_created else None,
                )

                zero_threshold = cold_counts.nh_zero_threshold.get()
                schema = cold_counts.nh_schema.get()
                zero_bucket = cold_counts.zero_bucket.get()

                negative_span, negative_delta = make_buckets(cold_counts.nh_buckets_negative)
                positive_span, positive_delta = make_buckets(cold_counts.nh_buckets_positive)






        # defer func() {
        #     coldCounts.nativeHistogramBucketsPositive.Range(addAndReset(&hotCounts.nativeHistogramBucketsPositive, &hotCounts.nativeHistogramBucketsNumber))
        #     coldCounts.nativeHistogramBucketsNegative.Range(addAndReset(&hotCounts.nativeHistogramBucketsNegative, &hotCounts.nativeHistogramBucketsNumber))
        # }()
        return (metric,)


class Info(MetricWrapperBase):
    """Info metric, key-value pairs.

     Examples of Info include:
        - Build information
        - Version information
        - Potential target metadata

     Example usage:
        from prometheus_client import Info

        i = Info('my_build', 'Description of info')
        i.info({'version': '1.2.3', 'buildhost': 'foo@bar'})

     Info metrics do not work in multiprocess mode.
    """
    _type = 'info'

    def _metric_init(self):
        self._labelname_set = set(self._labelnames)
        self._lock = Lock()
        self._value = {}

    def info(self, val: Dict[str, str]) -> None:
        """Set info metric."""
        if self._labelname_set.intersection(val.keys()):
            raise ValueError('Overlapping labels for Info metric, metric: {} child: {}'.format(
                self._labelnames, val))
        if any(i is None for i in val.values()):
            raise ValueError('Label value cannot be None')
        with self._lock:
            self._value = dict(val)

    def _child_samples(self) -> Iterable[PBMetric]:
        with self._lock:
            label_names = sorted(self._value.keys())
            label_values = [self._value[name] for name in label_names]
            return (make_untyped_metric(label_names=label_names, label_values=label_values, value=1),)


class Enum(MetricWrapperBase):
    """Enum metric, which of a set of states is true.

     Example usage:
        from prometheus_client import Enum

        e = Enum('task_state', 'Description of enum',
          states=['starting', 'running', 'stopped'])
        e.state('running')

     The first listed state will be the default.
     Enum metrics do not work in multiprocess mode.
    """
    _type = 'stateset'

    def __init__(self,
                 name: str,
                 documentation: str,
                 labelnames: Sequence[str] = (),
                 namespace: str = '',
                 subsystem: str = '',
                 unit: str = '',
                 registry: Optional[CollectorRegistry] = REGISTRY,
                 _labelvalues: Optional[Sequence[str]] = None,
                 states: Optional[Sequence[str]] = None,
                 ):
        super().__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            namespace=namespace,
            subsystem=subsystem,
            unit=unit,
            registry=registry,
            _labelvalues=_labelvalues,
        )
        if name in labelnames:
            raise ValueError(f'Overlapping labels for Enum metric: {name}')
        if not states:
            raise ValueError(f'No states provided for Enum metric: {name}')
        self._kwargs['states'] = self._states = states

    def _metric_init(self) -> None:
        self._value = 0
        self._lock = Lock()

    def state(self, state: str) -> None:
        """Set enum metric state."""
        self._raise_if_not_observable()
        with self._lock:
            self._value = self._states.index(state)

    def _child_samples(self) -> Iterable[PBMetric]:
        with self._lock:
            return [
                make_untyped_metric(
                    label_names=(self._name,),
                    label_values=(s,),
                    value=1 if i == self._value else 0
                ) for i, s in enumerate(self._states)
            ]
