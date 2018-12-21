package io.scalecube.services.transport.rsocket;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Iterator;
import java.util.concurrent.Executor;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;

/**
 * RSocket service transport. Entry point for getting {@link RSocketClientTransport} and {@link
 * RSocketServerTransport}.
 */
public class RSocketServiceTransport implements ServiceTransport {

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  @Override
  public ClientTransport getClientTransport(Executor workerThreadPool) {
    return new RSocketClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        DelegatedLoopResources.newClientLoopResources((EventLoopGroup) workerThreadPool));
  }

  @Override
  public ServerTransport getServerTransport(Executor workerThreadPool) {
    return new RSocketServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        (EventLoopGroup) workerThreadPool);
  }

  @Override
  public Executor getWorkerThreadPool(int numOfThreads) {
    return Epoll.isAvailable()
        ? new ExtendedEpollEventLoopGroup(numOfThreads, this::chooseEventLoop)
        : new ExtendedNioEventLoopGroup(numOfThreads, this::chooseEventLoop);
  }

  @Override
  public Mono<Void> shutdown(Executor workerPool) {
    //noinspection unchecked
    return Mono.defer(
        () ->
            workerPool != null
                ? FutureMono.from((Future) ((EventLoopGroup) workerPool).shutdownGracefully())
                : Mono.empty());
  }

  private EventLoop chooseEventLoop(Channel channel, Iterator<EventExecutor> executors) {
    while (executors.hasNext()) {
      EventExecutor eventLoop = executors.next();
      if (eventLoop.inEventLoop()) {
        return (EventLoop) eventLoop;
      }
    }
    return null;
  }
}
