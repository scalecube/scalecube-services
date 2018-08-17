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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.FutureMono;

public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";
  private static final String THREAD_FACTORY_POOL_NAME = "scalecube-rsocket";

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

  @Override
  public ClientTransport getClientTransport(ExecutorService executorService) {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    EventLoopGroup eventLoopGroup = (EventLoopGroup) executorService;
    return new RSocketClientTransport(new ServiceMessageCodec(headersCodec), eventLoopGroup);
  }

  @Override
  public ServerTransport getServerTransport(ExecutorService executorService) {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    EventLoopGroup eventLoopGroup = (EventLoopGroup) executorService;
    return new RSocketServerTransport(new ServiceMessageCodec(headersCodec), eventLoopGroup);
  }

  @Override
  public EventLoopGroup getExecutorService() {
    int nThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory(THREAD_FACTORY_POOL_NAME, true);
    return isEpollSupported
        ? new EpollEventLoopGroup(nThreads, threadFactory)
        : new NioEventLoopGroup(nThreads, threadFactory);
  }

  @Override
  public Mono<Void> shutdown(ExecutorService executorService) {
    return Mono.defer(
        () -> FutureMono.from((Future) ((EventLoopGroup) executorService).shutdownGracefully()));
  }
}
