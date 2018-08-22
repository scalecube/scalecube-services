package io.scalecube.services.transport.api;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

/**
 * Service transport worker thread chooser.
 */
public interface WorkerThreadChooser {

  /**
   * Function to select a worker executor from executor pool by connection parameters.
   *
   * @param id known identifier
   * @param localAddress local address of connection
   * @param remoteAddress remote address of connection
   * @param executors array of available executors
   * @return chosen executor
   */
  Executor getWorker(
      String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors);
}
