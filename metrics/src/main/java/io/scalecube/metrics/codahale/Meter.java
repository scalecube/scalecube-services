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
  public void mark(long num) {
    meter.mark(num);
  }
  
  @Override
  public long getCount() {
    return meter.getCount();
  }
}
