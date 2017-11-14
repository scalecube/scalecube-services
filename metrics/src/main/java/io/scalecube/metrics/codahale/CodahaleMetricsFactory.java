package io.scalecube.metrics.codahale;

import io.scalecube.metrics.api.Counter;
import io.scalecube.metrics.api.CounterFactory;
import io.scalecube.metrics.api.Gauge;
import io.scalecube.metrics.api.GaugeFactory;
import io.scalecube.metrics.api.HistogramFactory;
import io.scalecube.metrics.api.Meter;
import io.scalecube.metrics.api.MeterFactory;
import io.scalecube.metrics.api.MetricFactory;
import io.scalecube.metrics.api.Timer;
import io.scalecube.metrics.api.TimerFactory;

import com.codahale.metrics.MetricRegistry;

public class CodahaleMetricsFactory implements MetricFactory {

  private final MetricRegistry registry;

  private final MeterFactory meterFactory = new MeterFactory() {

    @Override
    public Meter get(final String component, final String methodName, final String eventType) {
      final com.codahale.metrics.Meter meter = registry.meter(MetricRegistry.name(component, methodName, eventType));
      return new io.scalecube.metrics.codahale.Meter(meter);
    }

    @Override
    public <T> Meter get(final Class<T> component, final String methodName, final String eventType) {
      return get(component.getName(), methodName, eventType);
    }
  };

  private final TimerFactory timerFactory = new TimerFactory() {

    @Override
    public Timer get(String component, String methodName) {
      final com.codahale.metrics.Timer timer = registry.timer(MetricRegistry.name(component, methodName));
      return new io.scalecube.metrics.codahale.Timer(timer);
    }

    @Override
    public <T> Timer get(Class<T> component, String methodName) {
      return get(component.getName(), methodName);
    }
  };

  private final CounterFactory counterFactory = new CounterFactory() {
    @Override
    public Counter get(final String component, final String methodName) {
      final com.codahale.metrics.Counter counter = registry.counter(MetricRegistry.name(component, methodName));
      return new io.scalecube.metrics.codahale.Counter(counter);
    }

    @Override
    public <T> Counter get(final Class<T> component, final String methodName) {
      return get(component.getName(), methodName);
    }
  };

  private final GaugeFactory gaugeFactory = new GaugeFactory() {
    @Override
    public <T> Gauge<T> register(final String component, final String methodName, final Gauge<T> gauge) {
      registry.register(MetricRegistry.name(component, methodName), new com.codahale.metrics.Gauge<T>() {
        @Override
        public T getValue() {
          return gauge.getValue();
        }
      });

      return gauge;
    }

    @Override
    public <T, G> Gauge<G> register(Class<T> component, String methodName, Gauge<G> gauge) {
      return register(component.getName(), methodName, gauge);
    }
  };

  private final HistogramFactory histogramFactory = new HistogramFactory() {
    @Override
    public Histogram get(final String component, final String methodName, final boolean biased) {
      final com.codahale.metrics.Histogram histogram = registry.histogram(MetricRegistry.name(component, methodName));
      return new io.scalecube.metrics.codahale.Histogram(histogram);
    }

    @Override
    public <T> Histogram get(final Class<T> component, final String methodName, final boolean biased) {
      return get(component.getName(), methodName, biased);
    }
  };

  public CodahaleMetricsFactory(final MetricRegistry registry) {
    this.registry = registry;
  }


  @Override
  public MeterFactory meter() {
    return this.meterFactory;
  }

  @Override
  public TimerFactory timer() {
    return this.timerFactory;
  }

  @Override
  public CounterFactory counter() {
    return counterFactory;
  }

  @Override
  public GaugeFactory gauge() {
    return gaugeFactory;
  }


  @Override
  public HistogramFactory histogram() {
    return this.histogramFactory;
  }
}
