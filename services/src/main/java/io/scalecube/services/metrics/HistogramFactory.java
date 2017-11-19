package io.scalecube.services.metrics;

import com.codahale.metrics.Histogram;

public interface HistogramFactory {

  Histogram get(String component, String methodName, boolean biased);

  <T> Histogram get(Class<T> component, String methodName, boolean biased);

}
