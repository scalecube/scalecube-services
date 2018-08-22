package io.scalecube.gateway.examples;

import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExamplesWorkerThreadChooser implements WorkerThreadChooser {

  private final Map<String, AtomicInteger> counterByRemoteHostAndId = new ConcurrentHashMap<>();

  @Override
  public Executor getWorker(
    String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
    AtomicInteger counter =
      counterByRemoteHostAndId.computeIfAbsent(id, key -> new AtomicInteger());
    int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % executors.length;
    return executors[index];
  }
}
