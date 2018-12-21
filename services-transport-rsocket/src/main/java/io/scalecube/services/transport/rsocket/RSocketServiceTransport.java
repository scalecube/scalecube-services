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
import java.util.Optional;
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
  public Resources resources(int numOfWorkers) {
    return new Resources(numOfWorkers);
  }

  @Override
  public ClientTransport clientTransport(ServiceTransport.Resources resources) {
    return new RSocketClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        DelegatedLoopResources.newClientLoopResources(((Resources) resources).workerPool));
  }

  @Override
  public ServerTransport serverTransport(ServiceTransport.Resources resources) {
    return new RSocketServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        ((Resources) resources).workerPool);
  }

  /** RSocket service transport Resources implementation. Holds inside custom EventLoopGroup. */
  private static class Resources implements ServiceTransport.Resources {

    private final EventLoopGroup workerPool;

    public Resources(int numOfWorkers) {
      workerPool =
          Epoll.isAvailable()
              ? new ExtendedEpollEventLoopGroup(numOfWorkers, this::chooseEventLoop)
              : new ExtendedNioEventLoopGroup(numOfWorkers, this::chooseEventLoop);
    }

    @Override
    public Optional<Executor> workerPool() {
      return Optional.of(workerPool);
    }

    @Override
    public Mono<Void> shutdown() {
      //noinspection unchecked
      return Mono.defer(
          () -> FutureMono.from((Future) ((EventLoopGroup) workerPool).shutdownGracefully()));
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
}
