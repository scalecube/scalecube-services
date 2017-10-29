package io.scalecube.metrics.api;

/**
 * Interface for abstracting the creation of well-formed Metrics-based objects. The consumer needs only to specify the
 * "allowable" fields (i.e., component and method) so that the metrics path is well defined (primarily concerned with
 * graphite trees).
 */
public interface MetricFactory {

  Timer createTimer(final String component, final String methodName);

  Counter createCounter(final String component, final String methodName);

  <T> Gauge<T> registerGauge(String component, String methodName, Gauge<T> metric);

  Meter createMeter(String component, String methodName, String eventType);

  Histogram createHistogram(String component, String methodName, boolean biased);
}
