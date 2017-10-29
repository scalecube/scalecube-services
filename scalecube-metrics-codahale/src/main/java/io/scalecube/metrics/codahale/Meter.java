package io.scalecube.metrics.codahale;

public class Meter implements io.scalecube.metrics.api.Meter {
  private final com.codahale.metrics.Meter meter;

  public Meter(com.codahale.metrics.Meter meter) {
    this.meter = meter;
  }

  @Override
  public void mark() {
    meter.mark();
  }

  @Override
  public void mark(long n) {
    meter.mark(n);
  }
}
