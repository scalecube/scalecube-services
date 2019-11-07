package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.netty.FutureMono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/** RSocket service transport. */
public class RSocketServiceTransport implements ServiceTransport {

  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance("application/json");
  private static final int NUM_OF_WORKERS = Runtime.getRuntime().availableProcessors();

  static {
    Hooks.onNextDropped(
        obj ->
            ReferenceCountUtil.safestRelease(
                obj instanceof ServiceMessage ? ((ServiceMessage) obj).data() : obj));
  }

  private int numOfWorkers = NUM_OF_WORKERS;
  private HeadersCodec headersCodec = HEADERS_CODEC;
  private Function<LoopResources, TcpServer> tcpServerProvider = defaultTcpServerProvider();
  private Function<LoopResources, TcpClient> tcpClientProvider = defaultTcpClientProvider();

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
    this.headersCodec = other.headersCodec;
    this.eventLoopGroup = other.eventLoopGroup;
    this.clientLoopResources = other.clientLoopResources;
    this.serverLoopResources = other.serverLoopResources;
  }

  /**
   * Sets a worker threads number.
   *
   * @param numOfWorkers number of worker threads
   * @return new {@code RSocketServiceTransport} instance
   */
  public RSocketServiceTransport numOfWorkers(int numOfWorkers) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.numOfWorkers = numOfWorkers;
    return rst;
  }

  /**
   * Sets a {@code HeadersCodec}.
   *
   * @param headersCodec headers codec
   * @return new {@code RSocketServiceTransport} instance
   */
  public RSocketServiceTransport headersCodec(HeadersCodec headersCodec) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.headersCodec = headersCodec;
    return rst;
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
   * Sets a provider function for custom {@code TcpClient}.
   *
   * @param factory {@code TcpClient} provider function
   * @return new {@code RSocketServiceTransport} instance
   */
  public RSocketServiceTransport tcpClient(Function<LoopResources, TcpClient> factory) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.tcpClientProvider = factory;
    return rst;
  }

  /**
   * Fabric method for client transport.
   *
   * @return client transport
   */
  @Override
  public ClientTransport clientTransport() {
    return new RSocketClientTransport(
        new ServiceMessageCodec(headersCodec), tcpClientProvider.apply(clientLoopResources));
  }

  /**
   * Fabric method for server transport.
   *
   * @return server transport
   */
  @Override
  public ServerTransport serverTransport() {
    return new RSocketServerTransport(
        new ServiceMessageCodec(headersCodec), tcpServerProvider.apply(serverLoopResources));
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
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);
    EventLoopGroup eventLoopGroup =
        Epoll.isAvailable()
            ? new EpollEventLoopGroup(numOfWorkers, threadFactory)
            : new NioEventLoopGroup(numOfWorkers, threadFactory);
    return LoopResources.colocate(eventLoopGroup);
  }

  private Mono<Void> shutdownEventLoopGroup() {
    //noinspection unchecked
    return Mono.defer(() -> FutureMono.from((Future) eventLoopGroup.shutdownGracefully()));
  }

  private Function<LoopResources, TcpServer> defaultTcpServerProvider() {
    return (LoopResources serverLoopResources) ->
        TcpServer.create()
            .runOn(serverLoopResources)
            .addressSupplier(() -> new InetSocketAddress(0));
  }

  private Function<LoopResources, TcpClient> defaultTcpClientProvider() {
    return (LoopResources clientLoopResources) ->
        TcpClient.newConnection().runOn(clientLoopResources);
  }

  @Override
  public String toString() {
    return "RSocketServiceTransport{"
        + "numOfWorkers="
        + numOfWorkers
        + ", headersCodec="
        + headersCodec
        + ", eventLoopGroup="
        + eventLoopGroup
        + ", clientLoopResources="
        + clientLoopResources
        + ", serverLoopResources="
        + serverLoopResources
        + '}';
  }
}
