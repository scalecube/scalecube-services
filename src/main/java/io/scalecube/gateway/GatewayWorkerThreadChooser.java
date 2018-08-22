package io.scalecube.gateway;

import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class GatewayWorkerThreadChooser implements WorkerThreadChooser {

  private final Map<Integer, WorkerThreadChooser> workerThreadChooserByListenPort;

  private GatewayWorkerThreadChooser(Builder builder) {
    this.workerThreadChooserByListenPort = new ConcurrentHashMap<>(builder.threadChooserMap);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Executor getWorker(
    String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
    if (!(localAddress instanceof InetSocketAddress)) {
      return null;
    }
    int listenPort = ((InetSocketAddress) localAddress).getPort();
    WorkerThreadChooser workerThreadChooser = workerThreadChooserByListenPort.get(listenPort);
    return workerThreadChooser != null
      ? workerThreadChooser.getWorker(id, localAddress, remoteAddress, executors)
      : null;
  }

  public static class Builder {

    private final Map<Integer, WorkerThreadChooser> threadChooserMap = new ConcurrentHashMap<>();

    private Builder() {
    }

    public Builder addCountingChooser(int listenPort) {
      addChooser(listenPort, new CountingWorkerThreadChooser());
      return this;
    }

    public Builder addRandomChooser(int listenPort) {
      addChooser(listenPort, new RandomWorkerThreadChooser());
      return this;
    }

    public Builder addChooser(int listenPort, WorkerThreadChooser threadChooser) {
      threadChooserMap.put(listenPort, threadChooser);
      return this;
    }

    public GatewayWorkerThreadChooser build() {
      return new GatewayWorkerThreadChooser(this);
    }
  }

  private static class CountingWorkerThreadChooser implements WorkerThreadChooser {

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public Executor getWorker(
      String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
      int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % executors.length;
      return executors[index];
    }
  }

  private static class RandomWorkerThreadChooser implements WorkerThreadChooser {

    @Override
    public Executor getWorker(
      String id, SocketAddress localAddress, SocketAddress remoteAddress, Executor[] executors) {
      int index = ThreadLocalRandom.current().nextInt(executors.length);
      ArrayList<Executor> list = new ArrayList<>(Arrays.asList(executors));
      Collections.shuffle(list);
      return list.get(index);
    }
  }
}
