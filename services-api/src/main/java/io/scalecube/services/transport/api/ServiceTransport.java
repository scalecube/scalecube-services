package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
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
   * Boolean function telling is native mode (such as epoll) supported.
   *
   * @return is native mode supported for service transport
   */
  boolean isNativeSupported();

  /**
   * Getting client transport.
   *
   * @param workerThreadPool service transport worker thread pool
   * @return client transport
   */
  ClientTransport getClientTransport(Executor workerThreadPool);

  /**
   * Getting server transport.
   *
   * @param workerThreadPool service transport worker thread pool
   * @return server transport
   */
  ServerTransport getServerTransport(Executor workerThreadPool);

  /**
   * Getting new service transport worker thread pool.
   *
   * @param numOfThreads number of threads for worker thread pool
   * @return executor
   */
  Executor getWorkerThreadPool(int numOfThreads);

  /**
   * Shutdowns service transport.
   *
   * @param workerThreadPool service transport worker thread pool
   * @return shutdown signal
   */
  Mono shutdown(Executor workerThreadPool);
}
