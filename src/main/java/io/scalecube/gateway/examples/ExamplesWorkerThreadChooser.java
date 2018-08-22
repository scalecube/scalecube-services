package io.scalecube.gateway.examples;

import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExamplesWorkerThreadChooser implements WorkerThreadChooser {

  private final Map<String, AtomicInteger> counterByRemoteHost = new ConcurrentHashMap<>();

  @Override
  public Executor getWorker(
    String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
    if (!(remoteAddress instanceof InetSocketAddress)) {
      return null;
    }
    String hostString = ((InetSocketAddress) remoteAddress).getHostString();
    AtomicInteger counter =
      counterByRemoteHost.computeIfAbsent(hostString, key -> new AtomicInteger());
    int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % executors.length;
    return executors[index];
  }
}
