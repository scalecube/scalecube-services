package io.scalecube.metrics.api;

public interface GaugeFactory {

  <T> Gauge<T> register(String component, String methodName, Gauge<T> gauge);

  <T,G> Gauge<G> register(Class<T> component, String methodName, Gauge<G> gauge);

}
