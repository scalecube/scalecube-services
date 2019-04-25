package io.scalecube.services.transport.api;

import java.util.Optional;
import java.util.concurrent.Executor;
import reactor.core.publisher.Mono;

/** Service transport resources interface. */
public interface TransportResources {

  TransportResources NO_OP_TRANSPORT_RESOURCES = new TransportResources() {};

  /**
   * Returns optional service transport worker thread pool.
   *
   * @return worker pool
   */
  default Optional<? extends Executor> workerPool() {
    return Optional.empty();
  }

  /**
   * Starts service transport resources.
   *
   * @return mono result
   */
  default Mono<? extends TransportResources> start() {
    return Mono.just(this);
  }

  /**
   * Shutdowns service transport resources.
   *
   * @return shutdown completion signal
   */
  default Mono<Void> shutdown() {
    return Mono.empty();
  }
}
