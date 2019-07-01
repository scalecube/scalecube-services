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
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

/** RSocket service transport. */
public class RSocketServiceTransport implements ServiceTransport {

  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance("application/json");
  private static final int NUM_OF_WORKERS = Runtime.getRuntime().availableProcessors();

  private int numOfWorkers = NUM_OF_WORKERS;
  private ServiceMessageCodec messageCodec = new ServiceMessageCodec(HEADERS_CODEC);
  private Function<LoopResources, TcpServer> tcpServerProvider = defaultTcpServerProvider();

  // resources
  private EventLoopGroup eventLoopGroup;
  private LoopResources clientLoopResources;
  private LoopResources serverLoopResources;

  /** Default constructor. */
  public RSocketServiceTransport() {}

  /**
   * Copy constructor.
   *
   * @param other other instance
   */
  private RSocketServiceTransport(RSocketServiceTransport other) {
    this.numOfWorkers = other.numOfWorkers;
    this.messageCodec = other.messageCodec;
    this.eventLoopGroup = other.eventLoopGroup;
    this.clientLoopResources = other.clientLoopResources;
    this.serverLoopResources = other.serverLoopResources;
  }

  /**
   * Sets a provider function for custom {@code TcpServer}.
   *
   * @param factory {@code TcpServer} provider function
   * @return new {@code RSocketServiceTransport} instance
   */
  public RSocketServiceTransport tcpServer(Function<LoopResources, TcpServer> factory) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.tcpServerProvider = factory;
    return rst;
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
    return new RSocketServerTransport(messageCodec, tcpServerProvider.apply(serverLoopResources));
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
    eventLoopGroup = newEventLoopGroup();
    clientLoopResources = DelegatedLoopResources.newClientLoopResources(eventLoopGroup);
    serverLoopResources = DelegatedLoopResources.newServerLoopResources(eventLoopGroup);
  }

  private EventLoopGroup newEventLoopGroup() {
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

  private Function<LoopResources, TcpServer> defaultTcpServerProvider() {
    return (LoopResources serverLoopResources) ->
        TcpServer.create()
            .runOn(serverLoopResources)
            .addressSupplier(() -> new InetSocketAddress(0));
  }
}
