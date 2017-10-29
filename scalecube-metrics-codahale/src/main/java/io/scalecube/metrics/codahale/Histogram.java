package io.scalecube.metrics.codahale;

public class Histogram implements io.scalecube.metrics.api.Histogram {
  private final com.codahale.metrics.Histogram histogram;

  public Histogram(com.codahale.metrics.Histogram histogram) {
    this.histogram = histogram;
  }

  @Override
  public void update(int value) {
    histogram.update(value);
  }

  @Override
  public void update(long value) {
    histogram.update(value);
  }
}
