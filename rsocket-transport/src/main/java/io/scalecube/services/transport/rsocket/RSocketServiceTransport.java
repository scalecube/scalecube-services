package io.scalecube.services.transport.rsocket;

import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.server.RSocketServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.ThreadFactory;

public class RSocketServiceTransport implements ServiceTransport {

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";
  private static final String THREAD_FACTORY_POOL_NAME = "scalecube-rsocket";

  private EventLoopGroup eventLoopGroup;

  public RSocketServiceTransport() {
    int nThreads = NettyRuntime.availableProcessors();
    ThreadFactory threadFactory = new DefaultThreadFactory(THREAD_FACTORY_POOL_NAME, true);

    eventLoopGroup = PlatformDependent.isWindows() ? new NioEventLoopGroup(nThreads, threadFactory)
        : new EpollEventLoopGroup(nThreads, threadFactory);
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

}
