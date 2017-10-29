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
   * @param num the amount by which the counter will be increased
   */
  public void inc(long num);

  /**
   * Decrement the counter by one.
   */
  public void dec();

  /**
   * Decrement the counter by {@code n}.
   *
   * @param num the amount by which the counter will be increased
   */
  public void dec(long num);

  /**
   * Returns the counter's current value.
   *
   * @return the counter's current value
   */
  public long getCount();

}
