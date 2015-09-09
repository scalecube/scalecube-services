package io.servicefabric.cluster;

import com.google.common.base.Preconditions;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * It is an utility wrapper class around {@code io.netty.util.HashedWheelTimer} which simplify its usage. This timer doesn't execute
 * scheduled job on time, but provide approximate execution and on each tick checks either there are any jobs behind the timer and execute
 * them. In most network applications, I/O timeout does not need to be accurate. The default tick duration is 100 milliseconds.
 *
 * @see io.netty.util.HashedWheelTimer
 */
final class TickingTimer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickingTimer.class);

  private final HashedWheelTimer hashedWheelTimer;
  private final ConcurrentMap<String, Timeout> tasks = new ConcurrentHashMap<>();

  /**
   * Creates instance of timer with default tick duration.
   */
  public TickingTimer() {
    hashedWheelTimer = new HashedWheelTimer();
  }

  /**
   * Creates instance of timer with th given tick duration.
   */
  public TickingTimer(Long tickDuration, TimeUnit timeUnit) {
    hashedWheelTimer = new HashedWheelTimer(tickDuration, timeUnit);
  }

  /**
   * Starts background timer's thread explicitly.
   */
  public void start() {
    hashedWheelTimer.start();
  }

  /**
   * Releases all resources acquired by this timer and cancels all jobs which were scheduled, but not executed yet.
   */
  public void stop() {
    hashedWheelTimer.stop();
    tasks.clear();
  }

  /**
   * Schedule runnable task to be executed after delay
   *
   * @param runnable task which will be executed after delay
   * @param delay delay to schedule
   * @param timeUnit time units for delay
   * @return Timeout object, which can be canceled. {@link io.netty.util.Timeout}
   */
  public Timeout schedule(final Runnable runnable, int delay, TimeUnit timeUnit) {
    Preconditions.checkNotNull(runnable);
    Preconditions.checkArgument(delay > 0);
    return hashedWheelTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        if (!timeout.isCancelled()) {
          runnable.run();
        }
      }
    }, delay, timeUnit);
  }

  /**
   * Schedule runnable task to be executed after delay, store timeout internally and map it to id. If task with {@code id} has been
   * previously scheduled, it would be cancelled and replaced with new one. Task can be canceled with id used
   * com.playtech.openapi.core.expiration.ITimerService#cancel(java.lang.String) method
   */
  public void schedule(final String id, final Runnable runnable, int delay, TimeUnit timeUnit) {
    Preconditions.checkArgument(id != null && !id.isEmpty());
    Preconditions.checkNotNull(runnable);
    Preconditions.checkArgument(delay > 0);
    final Timeout oldTask = tasks.remove(id);
    if (oldTask != null) {
      LOGGER.warn("Replacing previously scheduled task for id {} with new one.", id);
      oldTask.cancel();
    }
    Timeout timeout = hashedWheelTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        if (!timeout.isCancelled()) {
          runnable.run();
        }
        tasks.remove(id);
      }
    }, delay, timeUnit);
    tasks.put(id, timeout);
  }

  /**
   * Cancel timer task with specific id.
   */
  public void cancel(String id) {
    Timeout timeout = tasks.remove(id);
    if (timeout != null) {
      timeout.cancel();
    }
  }
}
