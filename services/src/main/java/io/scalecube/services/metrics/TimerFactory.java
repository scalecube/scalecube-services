package io.scalecube.services.metrics;

import com.codahale.metrics.Timer;

public interface TimerFactory {

  Timer get(String component, String methodName);

  <T> Timer get(Class<T> component, String methodName);

}
