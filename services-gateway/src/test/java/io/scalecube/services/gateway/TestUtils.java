package io.scalecube.services.gateway;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import reactor.core.publisher.Mono;

public final class TestUtils {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  private TestUtils() {}

  /**
   * Waits until the given condition is done
   *
   * @param condition condition
   * @return operation's result
   */
  public static Mono<Void> await(BooleanSupplier condition) {
    return Mono.delay(Duration.ofMillis(100)).repeat(() -> !condition.getAsBoolean()).then();
  }
}
