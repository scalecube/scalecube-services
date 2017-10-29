package io.scalecube.metrics.codahale;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.metrics.api.*;
import io.scalecube.metrics.api.Counter;
import io.scalecube.metrics.api.Gauge;
import io.scalecube.metrics.api.Histogram;
import io.scalecube.metrics.api.Meter;
import io.scalecube.metrics.api.Timer;

public class CodahaleMetricsFactory implements MetricFactory {
  private final MetricRegistry registry;

  public CodahaleMetricsFactory(final MetricRegistry registry) {
    this.registry = registry;
  }

  @Override
  public Timer createTimer(final String component, final String methodName) {
    final com.codahale.metrics.Timer timer = registry.timer(MetricRegistry.name(component, methodName));
    return new io.scalecube.metrics.codahale.Timer(timer);
  }

  @Override
  public Counter createCounter(final String component, final String methodName) {
    final com.codahale.metrics.Counter counter = registry.counter(MetricRegistry.name(component, methodName));
    return new io.scalecube.metrics.codahale.Counter(counter);
  }

  @Override
  public <T> Gauge<T> registerGauge(final String component, final String methodName, final Gauge<T> gauge) {
    registry.register(MetricRegistry.name(component, methodName), new com.codahale.metrics.Gauge<T>() {
      @Override
      public T getValue() {
        return gauge.getValue();
      }
    });

    return gauge;
  }

  @Override
  public Meter createMeter(final String component, final String methodName, final String eventType) {
    final com.codahale.metrics.Meter meter = registry.meter(MetricRegistry.name(component, methodName, eventType));
    return new io.scalecube.metrics.codahale.Meter(meter);
  }

  @Override
  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    final com.codahale.metrics.Histogram histogram = registry.histogram(MetricRegistry.name(component, methodName));
    return new io.scalecube.metrics.codahale.Histogram(histogram);
  }
}
