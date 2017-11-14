package io.scalecube.metrics.api;

import io.scalecube.metrics.api.Timer.Context;

public class Metrics {

  public static void mark(Meter meter) {
    if (meter != null) {
      meter.mark();
    }
  }

  public static void mark(MetricFactory metrics, Class component, String methodName, String eventType) {
    mark(metrics, component.getName(), methodName, eventType);
  }

  public static void mark(MetricFactory metrics, String component, String methodName, String eventType) {
    if (metrics != null) {
      mark(metrics.meter().get(component, methodName, eventType));
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
  public static Timer timer(MetricFactory metrics, String component, String methodName) {
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

  public static Counter counter(MetricFactory metrics, String component, String methodName) {
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
