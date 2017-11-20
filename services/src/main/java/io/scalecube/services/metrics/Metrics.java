package io.scalecube.services.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import java.lang.reflect.Method;

public class Metrics {

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
      registry.register(MetricRegistry.name(component, methodName), new Gauge<T>() {
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

  public Metrics(final MetricRegistry registry) {
    this.registry = registry;
  }

  public MeterFactory meter() {
    return this.meterFactory;
  }

  public TimerFactory timer() {
    return this.timerFactory;
  }

  public CounterFactory counter() {
    return counterFactory;
  }

  public GaugeFactory gauge() {
    return gaugeFactory;
  }

  public HistogramFactory histogram() {
    return this.histogramFactory;
  }
  
  public static void mark(Meter meter) {
    if (meter != null) {
      meter.mark();
    }
  }

  public static void mark(Metrics metrics, Class component, String methodName, String eventType) {
    mark(metrics, component.getName(), methodName, eventType);
  }

  public static void mark(Metrics metrics, String component, String methodName, String eventType) {
    if (metrics != null) {
      mark(metrics.meter().get(component, methodName, eventType));
    }
  }

  public static void mark(Class<?> serviceInterface, Metrics metrics, Method method, String eventType) {
    if (metrics != null) {
      Meter meter = metrics.meter().get(serviceInterface, method.getName(), eventType);
      Metrics.mark(meter);
    }
  }

  /**
   * if metrics is not null returns a Timer instance for a given component and method name.
   * 
   * @param metrics factory instance to get timer.
   * @param component name for the requested timer.
   * @param methodName for the requested timer.
   * @return timer instance.
   */
  public static Timer timer(Metrics metrics, String component, String methodName) {
    if (metrics != null) {
      return metrics.timer().get(component, methodName);
    } else {
      return null;
    }
  }

  public static Context time(Timer timer) {
    if (timer != null) {
      return timer.time();
    }
    return null;
  }

  public static void stop(Context ctx) {
    if (ctx != null) {
      ctx.stop();
    }
  }

  /**
   * if metrics is not null returns a Counter instance for a given component and method name.
   * 
   * @param metrics factory instance to get timer.
   * @param component name for the requested timer.
   * @param methodName for the requested timer.
   * @return counter instance.
   */
  public static Counter counter(Metrics metrics, String component, String methodName) {
    if (metrics != null) {
      return metrics.counter().get(component, methodName);
    } else {
      return null;
    }
  }

  public static void inc(Counter counter) {
    if (counter != null) {
      counter.inc();
    }
  }

  public static void dec(Counter counter) {
    if (counter != null) {
      counter.dec();
    }
  }
}
