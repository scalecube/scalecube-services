package io.scalecube.services.security;

import io.scalecube.security.tokens.jwt.KeyNotFoundException;
import java.time.Duration;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

public class RetryStrategies {

  private static final int MAX_ATTEMPTS = 20;
  private static final Duration MIN_BACKOFF = Duration.ofMillis(200);
  private static final Duration MAX_BACKOFF = Duration.ofSeconds(3);

  private RetryStrategies() {
    // Do not instantiate
  }

  /**
   * Returns zero-retries strategy.
   *
   * @return {@link Retry} instance
   */
  public static Retry noRetriesRetryStrategy() {
    return Retry.max(0);
  }

  /**
   * Returns retry-strategy which reacts on {@link KeyNotFoundException}.
   *
   * @return {@link RetryBackoffSpec} instance
   */
  public static RetryBackoffSpec keyNotFoundRetryStrategy() {
    return RetrySpec.backoff(MAX_ATTEMPTS, MIN_BACKOFF)
        .maxBackoff(MAX_BACKOFF)
        .filter(ex -> ex instanceof KeyNotFoundException);
  }

  /**
   * Returns retry-strategy which reacts on {@link KeyNotFoundException}.
   *
   * @param maxAttempts maxAttempts
   * @param minBackoff minBackoff
   * @param maxBackoff maxBackoff
   * @return {@link RetryBackoffSpec} instance
   */
  public static RetryBackoffSpec keyNotFoundRetryStrategy(
      int maxAttempts, Duration minBackoff, Duration maxBackoff) {
    return RetrySpec.backoff(maxAttempts, minBackoff)
        .maxBackoff(maxBackoff)
        .filter(ex -> ex instanceof KeyNotFoundException);
  }
}
