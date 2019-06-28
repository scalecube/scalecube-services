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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;

/** RSocket service transport. */
public class RSocketServiceTransport implements ServiceTransport {

  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance("application/json");
  private static final int NUM_OF_WORKERS = Runtime.getRuntime().availableProcessors();

  private final int numOfWorkers;
  private final ServiceMessageCodec messageCodec;

  // resources
  private EventLoopGroup eventLoopGroup;
  private LoopResources clientLoopResources;
  private LoopResources serverLoopResources;

  /** Default constructor. */
  public RSocketServiceTransport() {
    this(NUM_OF_WORKERS, HEADERS_CODEC);
  }

  /**
   * Constructor with DI.
   *
   * @param numOfWorkers number of tworker threads
   * @param headersCodec headers codec
   */
  public RSocketServiceTransport(int numOfWorkers, HeadersCodec headersCodec) {
    this.numOfWorkers = numOfWorkers;
    this.messageCodec = new ServiceMessageCodec(headersCodec);
  }

  /**
   * Fabric method for client transport.
   *
   * @return client transport
   */
  @Override
  public ClientTransport clientTransport() {
    return new RSocketClientTransport(messageCodec, clientLoopResources);
  }

  /**
   * Fabric method for server transport.
   *
   * @return server transport
   */
  @Override
  public ServerTransport serverTransport() {
    return new RSocketServerTransport(messageCodec, serverLoopResources);
  }

  @Override
  public Mono<RSocketServiceTransport> start() {
    return Mono.fromRunnable(this::start0).thenReturn(this);
  }

  @Override
  public Mono<Void> stop() {
    return Flux.concatDelayError(
            Mono.defer(() -> serverLoopResources.disposeLater()),
            Mono.defer(this::shutdownEventLoopGroup))
        .then();
  }

  private void start0() {
    eventLoopGroup = eventLoopGroup();
    clientLoopResources = DelegatedLoopResources.newClientLoopResources(eventLoopGroup);
    serverLoopResources = DelegatedLoopResources.newServerLoopResources(eventLoopGroup);
  }

  private EventLoopGroup eventLoopGroup() {
    return Epoll.isAvailable()
        ? new ExtendedEpollEventLoopGroup(numOfWorkers, this::chooseEventLoop)
        : new ExtendedNioEventLoopGroup(numOfWorkers, this::chooseEventLoop);
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

  private Mono<Void> shutdownEventLoopGroup() {
    //noinspection unchecked
    return Mono.defer(
        () -> FutureMono.from((Future) ((EventLoopGroup) eventLoopGroup).shutdownGracefully()));
  }
}
