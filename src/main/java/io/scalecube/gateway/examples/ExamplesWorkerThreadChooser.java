package io.scalecube.gateway.examples;

import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class ExamplesWorkerThreadChooser implements WorkerThreadChooser {

  private final Map<String, Integer> counterByRemoteHost = new ConcurrentHashMap<>();

  @Override
  public Executor getWorker(
    String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
    if (!(remoteAddress instanceof InetSocketAddress)) {
      return null;
    }
    String hostString = ((InetSocketAddress) remoteAddress).getHostString();
    int counter = counterByRemoteHost.compute(hostString, (key, i1) -> i1 != null ? i1 + 1 : 0);
    return executors[counter % executors.length];
  }
}
