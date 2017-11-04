package io.scalecube.metrics.api;

public interface CounterFactory {

  Counter get(String component, String methodName);

  <T> Counter get(Class<T> component, String methodName);

}
