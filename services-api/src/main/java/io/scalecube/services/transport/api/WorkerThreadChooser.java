package io.scalecube.services.transport.api;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

/** Service transport worker thread chooser. */
@FunctionalInterface
public interface WorkerThreadChooser {

  /**
   * Function to select a worker executor from executor pool by connection parameters.
   *
   * @param id an identifier of connection
   * @param localAddress local address of connection
   * @param remoteAddress remote address of connection
   * @param executors array of available executors
   * @return chosen executor from the available executors
   */
  Executor getWorker(
      String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors);
}
