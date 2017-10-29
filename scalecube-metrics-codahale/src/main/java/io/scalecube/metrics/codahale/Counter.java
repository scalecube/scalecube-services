package io.scalecube.metrics.codahale;

public class Counter implements io.scalecube.metrics.api.Counter {
  private final com.codahale.metrics.Counter counter;

  public Counter(com.codahale.metrics.Counter counter) {
    this.counter = counter;
  }

  @Override
  public void inc() {
    counter.inc();

  }

  @Override
  public void inc(long num) {
    counter.inc(num);
  }

  @Override
  public void dec() {
    counter.dec();
  }

  @Override
  public void dec(long num) {
    counter.dec(num);
  }

  @Override
  public long getCount() {
    return counter.getCount();
  }
}
