package io.scalecube.services.metrics;

import com.codahale.metrics.Counter;

public interface CounterFactory {

  Counter get(String component, String methodName);

  <T> Counter get(Class<T> component, String methodName);

}
