package io.scalecube.metrics.api;

/**
 * A metric which calculates the distribution of a value.
 */
public interface Histogram {

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(int value);

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(long value);

}
