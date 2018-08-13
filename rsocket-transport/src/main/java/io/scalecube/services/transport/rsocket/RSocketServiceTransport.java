package io.scalecube.services.transport.rsocket;

import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.server.RSocketServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.PlatformDependent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

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

  private EventLoopGroup eventLoopGroup;

  public RSocketServiceTransport() {
    int nThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory(THREAD_FACTORY_POOL_NAME, true);

    eventLoopGroup = isEpollSupported ? new EpollEventLoopGroup(nThreads, threadFactory)
        : new NioEventLoopGroup(nThreads, threadFactory);
  }

  @Override
  public ClientTransport getClientTransport() {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    return new RSocketClientTransport(new ServiceMessageCodec(headersCodec), eventLoopGroup);
  }

  @Override
  public ServerTransport getServerTransport() {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    return new RSocketServerTransport(new ServiceMessageCodec(headersCodec), eventLoopGroup);
  }

  @Override
  public EventLoopGroup getExecutorService() {
    return eventLoopGroup;
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(() -> FutureMono.from((Future) eventLoopGroup.shutdownGracefully()));
  }
}
