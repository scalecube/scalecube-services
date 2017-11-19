package io.scalecube.services.metrics;

import com.codahale.metrics.Gauge;

public interface GaugeFactory {

  <T> Gauge<T> register(String component, String methodName, Gauge<T> gauge);

  <T, G> Gauge<G> register(Class<T> component, String methodName, Gauge<G> gauge);

}
