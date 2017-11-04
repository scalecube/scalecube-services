package io.scalecube.metrics.api;

import io.scalecube.metrics.codahale.Histogram;

public interface HistogramFactory {

  Histogram get(String component, String methodName, boolean biased);

  <T> Histogram get(Class<T> component, String methodName, boolean biased);

}
