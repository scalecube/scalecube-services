package io.scalecube.metrics.api;

public interface MeterFactory {

  Meter get(String component, String methodName, String eventType);

  <T> Meter get(Class<T> component, String methodName, String eventType);
}
