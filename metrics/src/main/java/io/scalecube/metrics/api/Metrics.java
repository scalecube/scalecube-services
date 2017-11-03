package io.scalecube.metrics.api;

public class Metrics {

  public static void mark(Meter meter) {
    if (meter != null) {
      meter.mark();
    }
  }
}
