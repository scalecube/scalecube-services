package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import java.util.Optional;
import java.util.concurrent.Executor;
import reactor.core.publisher.Mono;

/** Service transport interface. */
public interface ServiceTransport {

  /**
   * Loads service transport from classpath service loader mechanism.
   *
   * @return instance of service transport
   */
  static ServiceTransport getTransport() {
    return ServiceLoaderUtil.findFirst(ServiceTransport.class)
        .orElseThrow(() -> new IllegalStateException("ServiceTransport not configured"));
  }

  /**
   * Provider of service transport resources.
   *
   * @param numOfWorkers num of worker threads
   * @return service transport resources
   */
  Resources resources(int numOfWorkers);

  /**
   * Provier of client transport.
   *
   * @param resources service transport resources obtained at {@link #resources(int)}
   * @return client transport
   */
  ClientTransport clientTransport(Resources resources);

  /**
   * Provider of server transport.
   *
   * @param resources service transport resources obtained at {@link #resources(int)}
   * @return server transport
   */
  ServerTransport serverTransport(Resources resources);

  /** Service transport resources interface. */
  interface Resources {

    /**
     * Returns optional service transport worker thread pool.
     *
     * @return worker pool; may be null
     */
    Optional<Executor> workerPool();

    /**
     * Shutdowns service transport resources created at {@link #resources(int)}.
     *
     * @return shutdown completion signal
     */
    Mono<Void> shutdown();
  }
}
