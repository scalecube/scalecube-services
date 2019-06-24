package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.Future;
import io.scalecube.services.transport.api.TransportResources;
import io.scalecube.services.transport.rsocket.experimental.tcp.ExtendedEpollEventLoopGroup;
import io.scalecube.services.transport.rsocket.experimental.tcp.ExtendedNioEventLoopGroup;
import java.util.Optional;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;

/** RSocket service transport Resources implementation. Holds inside custom EventLoopGroup. */
public class RSocketTransportResources implements TransportResources {

  private final int numOfWorkers;

  private EventLoopGroup eventLoopGroup;

  public RSocketTransportResources() {
    this(Runtime.getRuntime().availableProcessors());
  }

  public RSocketTransportResources(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }

  @Override
  public Optional<EventLoopGroup> workerPool() {
    return Optional.of(eventLoopGroup);
  }

  @Override
  public Mono<RSocketTransportResources> start() {
    return Mono.fromRunnable(() -> eventLoopGroup = eventLoopGroup()).thenReturn(this);
  }

  @Override
  public Mono<Void> shutdown() {
    //noinspection unchecked
    return Mono.defer(
        () -> FutureMono.from((Future) ((EventLoopGroup) eventLoopGroup).shutdownGracefully()));
  }

  private EventLoopGroup eventLoopGroup() {
    return Epoll.isAvailable()
        ? new ExtendedEpollEventLoopGroup(numOfWorkers)
        : new ExtendedNioEventLoopGroup(numOfWorkers);
  }
}
