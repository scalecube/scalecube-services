package io.scalecube.services.metrics;

import com.codahale.metrics.Meter;

public interface MeterFactory {

  Meter get(String component, String methodName, String eventType);

  <T> Meter get(Class<T> component, String methodName, String eventType);

}
