package io.scalecube.metrics.api;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A timer metric which aggregates timing durations and provides duration statistics, plus throughput statistics via
 * {@link Meter}.
 */
public interface Timer {

  /**
   * A timing context.
   *
   * @see com.outbrain.swinfra.metrics.api.Timer#time()
   */
  public static interface Context {

    /**
     * Stops recording the elapsed time and updates the timer.
     */
    public void stop();
  }

  /**
   * Adds a recorded duration.
   *
   * @param duration the length of the duration
   * @param unit the scale unit of {@code duration}
   */
  public void update(long duration, TimeUnit unit);

  /**
   * Times and records the duration of event.
   *
   * @param event a {@link Callable} whose {@link Callable#call()} method implements a process whose duration should be
   *        timed
   * @param <T> the type of the value returned by {@code event}
   * @return the value returned by {@code event}
   * @throws Exception if {@code event} throws an {@link Exception}
   */
  public <T> T time(Callable<T> event) throws Exception;

  /**
   * Returns a new {@link Context}.
   *
   * @return a new {@link Context}
   * @see Context
   */
  public Context time();
}
