package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.PlatformDependent;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.FutureMono;

public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  private static boolean preferEpoll = false;

  private static final String EPOLL_CLASS_NAME = "io.netty.channel.epoll.Epoll";

  static {
    if (PlatformDependent.isWindows()) {
      LOGGER.warn("Epoll is not supported by this environment, NIO will be used");
    } else {
      try {
        Class.forName(EPOLL_CLASS_NAME);
        preferEpoll = Epoll.isAvailable();
      } catch (ClassNotFoundException e) {
        LOGGER.warn("Cannot load Epoll, NIO will be used", e);
      }
    }
    LOGGER.debug("Epoll support: " + preferEpoll);
  }

  @Override
  public ClientTransport getClientTransport(
      Executor selectorThreadPool, Executor workerThreadPool) {

    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    DelegatedLoopResources loopResources =
        new DelegatedLoopResources(preferEpoll, selectorThreadPool, workerThreadPool);

    return new RSocketClientTransport(messageCodec, loopResources);
  }

  @Override
  public ServerTransport getServerTransport(
      Executor selectorThreadPool, Executor workerThreadPool) {

    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    DelegatedLoopResources loopResources =
        new DelegatedLoopResources(preferEpoll, selectorThreadPool, workerThreadPool);

    return new RSocketServerTransport(messageCodec, loopResources);
  }

  @Override
  public Executor getSelectorThreadPool() {
    int bossThreads = 1;
    DefaultThreadFactory threadFactory = new DefaultThreadFactory("rsocket-boss", true);

    return preferEpoll
        ? new EpollEventLoopGroup(bossThreads, threadFactory)
        : new NioEventLoopGroup(bossThreads, threadFactory);
  }

  @Override
  public Executor getWorkerThreadPool(WorkerThreadChooser threadChooser) {
    int workerThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);

    EventExecutorChooser executorChooser =
        threadChooser != null
            ? new DefaultEventExecutorChooser(threadChooser)
            : EventExecutorChooser.NULL_INSTANCE;

    return preferEpoll
        ? new ExtendedEpollEventLoopGroup(workerThreads, threadFactory, executorChooser)
        : new ExtendedNioEventLoopGroup(workerThreads, threadFactory, executorChooser);
  }

  @Override
  public Mono<Void> shutdown(Executor selectorThreadPool, Executor workerThreadPool) {
    //noinspection unchecked
    return Mono.defer(
        () ->
            Mono.when(
                Stream.of(selectorThreadPool, workerThreadPool)
                    .filter(Objects::nonNull)
                    .map(executor -> (EventLoopGroup) executor)
                    .map(
                        loopGroup ->
                            FutureMono.deferFuture(
                                () -> (Future<Void>) loopGroup.shutdownGracefully()))
                    .toArray(Mono[]::new)));
  }
}
