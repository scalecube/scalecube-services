package io.scalecube.metrics.api;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute exponentially-weighted moving
 * average throughput.
 */
public interface Meter {

  /**
   * Mark the occurrence of an event.
   */
  public void mark();

  /**
   * Mark the occurrence of a given number of events.
   *
   * @param num the number of events
   */
  public void mark(long num);
}
