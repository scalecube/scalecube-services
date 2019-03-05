package io.scalecube.services.transport.api;

import java.util.Optional;
import java.util.concurrent.Executor;
import reactor.core.publisher.Mono;

/** Service transport resources interface. */
public interface TransportResources {

  /**
   * Returns optional service transport worker thread pool.
   *
   * @return worker pool
   */
  Optional<? extends Executor> workerPool();

  /**
   * Starts service transport resources.
   *
   * @return mono result
   */
  Mono<? extends TransportResources> start();

  /**
   * Shutdowns service transport resources.
   *
   * @return shutdown completion signal
   */
  Mono<Void> shutdown();
}
