package io.scalecube.concurrency;


import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Futures {

  /**
   * used to complete the request future with timeout exception in case no response comes from service.
   */
  private static final ScheduledExecutorService delayer =
      ThreadFactory.singleScheduledExecutorService(ThreadFactory.SC_SERVICES_TIMEOUT);
  
  /**
   * Create a future and apply give timeout.
   * 
   * @param resultFuture give future to apply timeout.
   * @param timeout for completion  success / error 
   * @return future with timeout.
   */
  public static <T> CompletableFuture<T> withTimeout(final CompletableFuture<T> resultFuture,
      Duration timeout) {

    if (!resultFuture.isDone()) {
      // schedule to terminate the target goal in future in case it was not done yet
      delayer.schedule(() -> {
        // by this time the target goal should have finished.
        if (!resultFuture.isDone()) {
          // target goal not finished in time so cancel it with timeout.
          resultFuture.completeExceptionally(new TimeoutException("expecting response reached timeout!"));
        }
      }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
    return resultFuture;
  }
  
}
