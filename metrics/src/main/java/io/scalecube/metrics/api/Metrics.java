package io.scalecube.metrics.api;

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
}
