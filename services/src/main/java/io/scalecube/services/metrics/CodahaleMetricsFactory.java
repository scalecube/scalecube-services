package io.scalecube.services.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class CodahaleMetricsFactory implements MetricFactory {

  private final MetricRegistry registry;


  private final MeterFactory meterFactory = new MeterFactory() {

    @Override
    public Meter get(final String component, final String methodName, final String eventType) {
      return registry.meter(MetricRegistry.name(component, methodName, eventType));
    }

    @Override
    public <T> Meter get(final Class<T> component, final String methodName, final String eventType) {
      return get(component.getName(), methodName, eventType);
    }
  };

  private final TimerFactory timerFactory = new TimerFactory() {

    @Override
    public Timer get(String component, String methodName) {
      return registry.timer(MetricRegistry.name(component, methodName));
    }

    @Override
    public <T> Timer get(Class<T> component, String methodName) {
      return get(component.getName(), methodName);
    }
  };

  private final CounterFactory counterFactory = new CounterFactory() {
    @Override
    public Counter get(final String component, final String methodName) {
      return registry.counter(MetricRegistry.name(component, methodName));
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
      return registry.histogram(MetricRegistry.name(component, methodName));
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
