package io.scalecube.services.transport.rsocket;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import io.netty.util.internal.PlatformDependent;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.nio.channels.spi.SelectorProvider;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
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
  public ClientTransport getClientTransport(ExecutorService selectorExecutor,
      ExecutorService workerExecutor) {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    EventLoopGroup bossEventLoopGroup = (EventLoopGroup) selectorExecutor;
    EventLoopGroup workerEventLoopGroup = (EventLoopGroup) workerExecutor;
    return new RSocketClientTransport(new ServiceMessageCodec(headersCodec), bossEventLoopGroup,
        workerEventLoopGroup);
  }

  @Override
  public ServerTransport getServerTransport(ExecutorService selectorExecutor,
      ExecutorService workerExecutor) {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    EventLoopGroup bossEventLoopGroup = (EventLoopGroup) selectorExecutor;
    EventLoopGroup workerEventLoopGroup = (EventLoopGroup) workerExecutor;
    return new RSocketServerTransport(new ServiceMessageCodec(headersCodec), bossEventLoopGroup,
        workerEventLoopGroup);
  }

  @Override
  public EventLoopGroup getSelectorExecutor() {
    int bossThreads = 1;
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-boss", true);
    ThreadPerTaskExecutor threadPerTaskExecutor = new ThreadPerTaskExecutor(threadFactory);

    EventExecutorChooserFactory eventLoopChooserFactory =
        new EventExecutorChooserFactory() {

          private ThreadLocal<AtomicInteger> threadLocalCounter =
              ThreadLocal.withInitial(AtomicInteger::new);

          @Override
          public EventExecutorChooser newChooser(EventExecutor[] executors) {
            return () -> {
              int counter = threadLocalCounter.get().getAndIncrement() & Integer.MAX_VALUE;
              return executors[counter % executors.length];
            };
          }
        };

    return isEpollSupported
        ? new EpollEventLoopGroup(bossThreads, threadPerTaskExecutor, eventLoopChooserFactory,
        DefaultSelectStrategyFactory.INSTANCE)
        : new NioEventLoopGroup(bossThreads, threadPerTaskExecutor, eventLoopChooserFactory,
            SelectorProvider.provider(), DefaultSelectStrategyFactory.INSTANCE);
  }

  @Override
  public EventLoopGroup getWorkerExecutor() {
    int workerThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);

    return isEpollSupported
        ? new EpollEventLoopGroup(workerThreads, threadFactory)
        : new NioEventLoopGroup(workerThreads, threadFactory);
  }

  @Override
  public Mono<Void> shutdown(ExecutorService selectorExecutor, ExecutorService workerExecutor) {
    //noinspection unchecked
    return Mono.defer(() ->
        Mono.when(
            Stream.of(selectorExecutor, workerExecutor)
                .filter(Objects::nonNull)
                .map(executor -> (EventLoopGroup) executor)
                .map(loopGroup ->
                    FutureMono.deferFuture(() -> (Future<Void>) loopGroup.shutdownGracefully()))
                .toArray(Mono[]::new)
        )
    );
  }
}
