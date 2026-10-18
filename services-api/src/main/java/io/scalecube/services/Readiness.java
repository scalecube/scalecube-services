package io.scalecube.services;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Waits until every {@link ReadinessCondition} is met, polling each on a background thread, then
 * runs a readiness hook.
 *
 * <p>When every condition is met, the readiness hook runs and {@link #future()} completes. When a
 * condition throws, polling of that condition stops, {@link #future()} completes exceptionally and
 * the hook does not run. When the readiness is {@link #close() closed} before every condition is
 * met, {@link #future()} is cancelled and the hook does not run.
 */
public class Readiness implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Readiness.class);

  private static final int RETRY_CLOSE_TIMEOUT_MS = 5000;
  private static final int LOG_INTERVAL_MS = 5000;
  private static final long POLL_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);

  private final ExecutorService readinessExecutor =
      Executors.newCachedThreadPool(r -> new Thread(r, "readiness-executor"));
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CompletableFuture<Void> future;

  private Readiness(
      long logIntervalMs, List<ReadinessCondition> conditions, Runnable readinessHook) {
    try {
      future =
          CompletableFuture.allOf(
                  conditions.stream()
                      .map(condition -> runAsync(condition, logIntervalMs, readinessExecutor))
                      .toArray(CompletableFuture[]::new))
              .thenAccept(
                  unused -> {
                    // A poll loop interrupted by close() returns normally, which is not readiness
                    if (!closed.get()) {
                      runReadinessHook(readinessHook);
                    }
                  });
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the future of this readiness: it completes after every condition is met and the
   * readiness hook has run, completes exceptionally when a condition throws, and is cancelled when
   * the readiness is closed before that.
   *
   * @return future of this readiness
   */
  public Future<Void> future() {
    return future;
  }

  /**
   * Starts polling the given conditions, logging a waiting condition every 5 seconds.
   *
   * @param conditions conditions to wait for
   * @param readinessHook runs once every condition is met; may be {@code null}. An exception it
   *     throws is logged, not propagated.
   * @return started readiness; close it to stop polling
   */
  public static Readiness start(List<ReadinessCondition> conditions, Runnable readinessHook) {
    return start(LOG_INTERVAL_MS, conditions, readinessHook);
  }

  /**
   * Starts polling the given conditions.
   *
   * @param logIntervalMs how often, in milliseconds, a condition still waiting is logged
   * @param conditions conditions to wait for
   * @param readinessHook runs once every condition is met; may be {@code null}. An exception it
   *     throws is logged, not propagated.
   * @return started readiness; close it to stop polling
   */
  public static Readiness start(
      long logIntervalMs, List<ReadinessCondition> conditions, Runnable readinessHook) {
    return new Readiness(logIntervalMs, conditions, readinessHook);
  }

  private static void runReadinessHook(Runnable readinessHook) {
    if (readinessHook != null) {
      try {
        readinessHook.run();
      } catch (Exception e) {
        LOGGER.error("Exception occurred: ", e);
      }
    }
  }

  private static CompletableFuture<?> runAsync(
      ReadinessCondition condition, long logIntervalMs, ExecutorService executor) {
    return CompletableFuture.runAsync(toRunnable(condition, logIntervalMs), executor)
        .whenComplete(
            (unused, ex) -> {
              if (ex != null) {
                LOGGER.error("[{}] Exception occurred: ", condition.name(), ex);
              }
            });
  }

  private static Runnable toRunnable(ReadinessCondition condition, long logIntervalMs) {
    return () -> {
      long lastTime = 0;
      final var name = condition.name();

      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          LOGGER.warn("[{}] Interrupted", name);
          return;
        }

        if (condition.getAsBoolean()) {
          LOGGER.debug("[{}] Condition met", name);
          return;
        }

        final var now = System.currentTimeMillis();
        if (now - lastTime > logIntervalMs) {
          LOGGER.debug("[{}] Waiting for condition ...", name);
          lastTime = now;
        }

        LockSupport.parkNanos(POLL_INTERVAL_NS);
      }
    };
  }

  /**
   * Stops polling: interrupts the background threads and waits until they have finished, retrying
   * the wait every 5 seconds. When called before every condition is met, {@link #future()} is
   * cancelled and the readiness hook does not run.
   */
  @Override
  public void close() {
    closed.set(true);
    if (future != null) {
      future.cancel(true);
    }
    try {
      while (true) {
        readinessExecutor.shutdownNow();
        if (!readinessExecutor.awaitTermination(RETRY_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          LOGGER.warn("Retrying close ...");
        } else {
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Failed to close due to thread interrupt");
    }
  }

  /**
   * Named {@link BooleanSupplier} used as readiness check. Readiness-condition is considered
   * satisfied when {@link #getAsBoolean()} returns {@code true}.
   */
  public interface ReadinessCondition extends BooleanSupplier {

    /**
     * Returns name of this condition, used for logging.
     *
     * @return condition name
     */
    String name();
  }
}
