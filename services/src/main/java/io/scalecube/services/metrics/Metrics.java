package io.scalecube.services.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

public class Metrics {

  private final MeterRegistry registry;

  public Metrics(final MeterRegistry registry) {
    this.registry = registry;
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
      return metrics.getTimer(component, methodName);
    } else {
      return null;
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
      return metrics.getCounter(component, methodName);
    } else {
      return null;
    }
  }

  /**
   * Reports an event to given <code>meter</code>.
   *
   * @param counter - meter that consumes na event.
   */
  public static void increment(Counter counter) {
    if (counter != null) {
      counter.increment();
    }
  }

  /**
   * Reports an event to <code>meter</code> described by given parameters.
   *
   * @param metrics - {@link Metrics} instance with {@link MeterRegistry} initialized.
   * @param component - part of metric description (calss name is used).
   * @param methodName - part of metric description.
   * @param eventType - part of metric description.
   */
  public static void increment(
      Metrics metrics, Class component, String methodName, String eventType) {
    increment(metrics, component.getName(), methodName, eventType);
  }

  /**
   * Reports an event to <code>meter</code> described by given parameters.
   *
   * @param metrics - {@link Metrics} instance with {@link MeterRegistry} initialized.
   * @param component - part of metric description.
   * @param methodName - part of metric description.
   * @param eventType - part of metric description.
   */
  public static void increment(
      Metrics metrics, String component, String methodName, String eventType) {
    if (metrics != null) {
      increment(metrics.getCounter(component, methodName, eventType));
    }
  }

  /**
   * Reports an event to <code>meter</code> described by given parameters.
   *
   * @param serviceInterface - part of metric description (class name is used).
   * @param metrics - {@link Metrics} instance with {@link
   *     io.micrometer.core.instrument.MeterRegistry} initialized.
   * @param method - part of metric description (method name is used).
   * @param eventType - part of metric description.
   */
  public static void increment(
      Class<?> serviceInterface, Metrics metrics, Method method, String eventType) {
    if (metrics != null) {
      Counter meter = metrics.getCounter(serviceInterface, method.getName(), eventType);
      Metrics.increment(meter);
    }
  }

  /**
   * Report the interval to metrics.
   *
   * @param timer - timer to report
   * @param duration result to record
   */
  public static void record(Timer timer, Duration duration) {
    if (timer != null) {
      timer.record(duration);
    }
  }

  /**
   * Increase counter.
   *
   * @param counter - counter to increase.
   */
  public static void inc(Counter counter) {
    if (counter != null) {
      counter.increment();
    }
  }

  public Timer getTimer(String component, String methodName) {
    return registry.timer(new StringJoiner(".").add(component).add(methodName).toString());
  }

  public <T> Timer getTimer(Class<T> component, String methodName) {
    return getTimer(component.getName(), methodName);
  }

  public Counter getCounter(
      final String component, final String methodName, final String eventType) {
    return registry.counter(
        new StringJoiner(".").add(component).add(methodName).add(eventType).toString());
  }

  public Counter getCounter(final String component, final String methodName) {
    return registry.counter(new StringJoiner(".").add(component).add(methodName).toString());
  }

  public <T> Counter getCounter(final Class<T> component, final String methodName) {
    return getCounter(component.getName(), methodName);
  }

  public <T> Counter getCounter(
      final Class<T> component, final String methodName, final String eventType) {
    return getCounter(component.getName(), methodName, eventType);
  }

  public LongAdder gauge(LongAdder l, Class<?> component, String methodName) {
    return gauge(l, component.getName(), methodName);
  }

  public LongAdder gauge(LongAdder l, String component, String methodName) {
    return registry.gauge(new StringJoiner(".").add(component).add(methodName).toString(), l);
  }

  public DistributionSummary getHistogram(
      final String component, final String methodName, final boolean biased) {
    return registry.summary(new StringJoiner(",").add(component).add(methodName).toString());
  }

  public <T> DistributionSummary getHistogram(
      final Class<T> component, final String methodName, final boolean biased) {
    return getHistogram(component.getName(), methodName, biased);
  }
}
