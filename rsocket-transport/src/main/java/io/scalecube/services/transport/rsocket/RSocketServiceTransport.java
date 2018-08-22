package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.internal.PlatformDependent;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.lang.reflect.Constructor;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.FutureMono;

public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  private static boolean isEpollSupported = false;

  static {
    if (PlatformDependent.isWindows()) {
      LOGGER.warn("Epoll is not supported by this environment, NIO will be used");
    } else {
      try {
        Class.forName("io.netty.channel.epoll.Epoll");
        isEpollSupported = Epoll.isAvailable();
      } catch (ClassNotFoundException e) {
        LOGGER.warn("Cannot load Epoll, NIO will be used", e);
      }
    }
    LOGGER.debug("Epoll support: " + isEpollSupported);
  }

  public static boolean isEpollSupported() {
    return isEpollSupported;
  }

  @Override
  public ClientTransport getClientTransport(
      Executor selectorThreadPool, Executor workerThreadPool) {

    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    EventLoopGroup bossGroup = (EventLoopGroup) selectorThreadPool;
    EventLoopGroup workerGroup = (EventLoopGroup) workerThreadPool;

    return new RSocketClientTransport(messageCodec, bossGroup, workerGroup);
  }

  @Override
  public ServerTransport getServerTransport(
      Executor selectorThreadPool, Executor workerThreadPool) {

    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    ServiceMessageCodec messageCodec = new ServiceMessageCodec(headersCodec);

    EventLoopGroup bossGroup = (EventLoopGroup) selectorThreadPool;
    EventLoopGroup workerGroup = (EventLoopGroup) workerThreadPool;

    return new RSocketServerTransport(messageCodec, bossGroup, workerGroup);
  }

  @Override
  public Executor getSelectorThreadPool() {
    int bossThreads = 1;
    DefaultThreadFactory threadFactory = new DefaultThreadFactory("rsocket-boss", true);

    return isEpollSupported()
        ? new EpollEventLoopGroup(bossThreads, threadFactory)
        : new NioEventLoopGroup(bossThreads, threadFactory);
  }

  @Override
  public Executor getWorkerThreadPool(WorkerThreadChooser workerThreadChooser) {
    int workerThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);
    return isEpollSupported()
        ? new CustomEpollEventLoopGroup(
        workerThreads,
        threadFactory,
        workerThreadChooser,
        0,
            DefaultSelectStrategyFactory.INSTANCE,
        RejectedExecutionHandlers.reject())
        : new CustomNioEventLoopGroup(workerThreads, threadFactory, workerThreadChooser);
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

  private static class CustomEpollEventLoopGroup extends MultithreadEventLoopGroup {

    private final WorkerThreadChooser workerThreadChooser;

    public CustomEpollEventLoopGroup(
        int workerThreads,
        ThreadFactory threadFactory,
        WorkerThreadChooser workerThreadChooser,
        Object... args) {
      super(workerThreads, threadFactory, args);
      this.workerThreadChooser = workerThreadChooser;
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
      Class<?> eventLoopClass = Class.forName("io.netty.channel.epoll.EpollEventLoop");
      Constructor<?> constructor =
          eventLoopClass.getDeclaredConstructor(
              EventLoopGroup.class,
              Executor.class,
              int.class,
              SelectStrategy.class,
              RejectedExecutionHandler.class);
      constructor.setAccessible(true);
      return (EventLoop)
          constructor.newInstance(
              this,
              executor,
              (Integer) args[0],
              ((SelectStrategyFactory) args[1]).newSelectStrategy(),
              (RejectedExecutionHandler) args[2]);
    }

    @Override
    public ChannelFuture register(Channel channel) {
      if (workerThreadChooser == null) {
        return super.register(channel);
      }

      Executor[] executors = StreamSupport.stream(spliterator(), false).toArray(Executor[]::new);
      String channelId = channel.id().asLongText();
      SocketAddress localAddress = channel.localAddress();
      SocketAddress remoteAddress = channel.remoteAddress();
      EventLoop eventLoop =
          (EventLoop)
              workerThreadChooser.getWorker(channelId, localAddress, remoteAddress, executors);

      return eventLoop != null ? eventLoop.register(channel) : super.register(channel);
    }

    @Override
    public ChannelFuture register(ChannelPromise promise) {
      return register(promise.channel());
    }
  }

  private static class CustomNioEventLoopGroup extends NioEventLoopGroup {

    private final WorkerThreadChooser workerThreadChooser;

    public CustomNioEventLoopGroup(
        int workerThreads, ThreadFactory threadFactory, WorkerThreadChooser workerThreadChooser) {
      super(workerThreads, threadFactory);
      this.workerThreadChooser = workerThreadChooser;
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
      Class<?> eventLoopClass = Class.forName("io.netty.channel.nio.NioEventLoop");
      Constructor<?> constructor =
          eventLoopClass.getDeclaredConstructor(
              NioEventLoopGroup.class,
              Executor.class,
              SelectorProvider.class,
              SelectStrategy.class,
              RejectedExecutionHandler.class);
      constructor.setAccessible(true);
      return (EventLoop)
          constructor.newInstance(
              this,
              executor,
              (SelectorProvider) args[0],
              ((SelectStrategyFactory) args[1]).newSelectStrategy(),
              (RejectedExecutionHandler) args[2]);
    }

    @Override
    public ChannelFuture register(Channel channel) {
      if (workerThreadChooser == null) {
        return super.register(channel);
      }

      Executor[] executors = StreamSupport.stream(spliterator(), false).toArray(Executor[]::new);
      String channelId = channel.id().asLongText();
      SocketAddress localAddress = channel.localAddress();
      SocketAddress remoteAddress = channel.remoteAddress();
      EventLoop eventLoop =
          (EventLoop)
              workerThreadChooser.getWorker(channelId, localAddress, remoteAddress, executors);

      return eventLoop != null ? eventLoop.register(channel) : super.register(channel);
    }

    @Override
    public ChannelFuture register(ChannelPromise promise) {
      return register(promise.channel());
    }
  }
}
