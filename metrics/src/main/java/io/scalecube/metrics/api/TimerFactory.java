package io.scalecube.metrics.api;

public interface TimerFactory {

  Timer get(final String component, final String methodName);

  <T> Timer get(Class<T> component, String methodName);

}
