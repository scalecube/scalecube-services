package io.scalecube.metrics.api;

/**
 * An incrementing and decrementing counter metric.
 */
public interface Counter {
  /**
   * Increment the counter by one.
   */
  public void inc();

  /**
   * Increment the counter by {@code n}.
   *
   * @param n the amount by which the counter will be increased
   */
  public void inc(long n);

  /**
   * Decrement the counter by one.
   */
  public void dec();

  /**
   * Decrement the counter by {@code n}.
   *
   * @param n the amount by which the counter will be increased
   */
  public void dec(long n);

  /**
   * Returns the counter's current value.
   *
   * @return the counter's current value
   */
  public long getCount();

}
