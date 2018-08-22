package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.internal.PlatformDependent;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.WorkerThreadChooser;
import java.lang.reflect.Method;
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
    return createEventLoopGroup(2, new DefaultThreadFactory("rsocket-boss", true));
  }

  @Override
  public Executor getWorkerThreadPool(WorkerThreadChooser workerThreadChooser) {
    int workerThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);

    MultithreadEventLoopGroup eventLoopGroupTemplate =
        createEventLoopGroup(workerThreads, threadFactory);

    try {
      Object[] args =
          new Object[]{
              isEpollSupported() ? 0 : SelectorProvider.provider(),
              DefaultSelectStrategyFactory.INSTANCE,
              RejectedExecutionHandlers.reject()
          };

      return new MultithreadEventLoopGroup(workerThreads, threadFactory, args) {
        @Override
        protected EventLoop newChild(Executor executor, Object... args) throws Exception {
          return invokerNewChild(eventLoopGroupTemplate, executor, args);
        }

        @Override
        public ChannelFuture register(Channel channel) {
          if (workerThreadChooser == null) {
            return super.register(channel);
          }

          Executor[] executors =
              StreamSupport.stream(spliterator(), false).toArray(Executor[]::new);
          EventLoop eventLoop = chooseEventLoop(channel, workerThreadChooser, executors);

          return eventLoop != null ? eventLoop.register(channel) : super.register(channel);
        }

        @Override
        public ChannelFuture register(ChannelPromise promise) {
          return register(promise.channel());
        }
      };
    } finally {
      eventLoopGroupTemplate.shutdownGracefully(); // no need in this thread pool anymore
    }
  }

  private EventLoop chooseEventLoop(
      Channel channel, WorkerThreadChooser workerThreadChooser, Executor[] executors) {

    String channelId = channel.id().asLongText();
    SocketAddress localAddress = channel.localAddress();
    SocketAddress remoteAddress = channel.remoteAddress();

    return (EventLoop)
        workerThreadChooser.getWorker(channelId, localAddress, remoteAddress, executors);
  }

  private EventLoop invokerNewChild(
      MultithreadEventLoopGroup eventLoopGroupInstance, Executor executor, Object[] args)
      throws ReflectiveOperationException {
    Method newChild =
        eventLoopGroupInstance
            .getClass()
            .getDeclaredMethod("newChild", Executor.class, Object[].class);
    newChild.setAccessible(true);
    return (EventLoop) newChild.invoke(eventLoopGroupInstance, executor, args);
  }

  private MultithreadEventLoopGroup createEventLoopGroup(
      int numOfThreads, ThreadFactory threadFactory) {
    return isEpollSupported()
        ? new EpollEventLoopGroup(numOfThreads, threadFactory)
        : new NioEventLoopGroup(numOfThreads, threadFactory);
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
