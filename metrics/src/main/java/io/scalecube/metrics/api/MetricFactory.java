package io.scalecube.metrics.api;

/**
 * Interface for abstracting the creation of well-formed Metrics-based objects. The consumer needs only to specify the
 * "allowable" fields (i.e., component and method) so that the metrics path is well defined (primarily concerned with
 * graphite trees).
 */
public interface MetricFactory {
 
  GaugeFactory gauge();
  
  MeterFactory meter();

  TimerFactory timer();
  
  CounterFactory counter();

  HistogramFactory histogram();
  
}
