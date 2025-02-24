package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServerTransport.Authenticator;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;
import reactor.netty.channel.AbortedException;
import reactor.netty.resources.LoopResources;

public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  static {
    Hooks.onErrorDropped(
        t -> {
          if (AbortedException.isConnectionReset(t)
              || ConnectionClosedException.isConnectionClosed(t)) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Connection aborted: {}", t.toString());
            }
          }
        });
  }

  private int numOfWorkers = Runtime.getRuntime().availableProcessors();
  private HeadersCodec headersCodec = HeadersCodec.DEFAULT_INSTANCE;
  private Collection<DataCodec> dataCodecs = DataCodec.getAllInstances();
  private CredentialsSupplier credentialsSupplier;
  private Authenticator authenticator;
  private List<String> allowedRoles;

  private Function<LoopResources, RSocketServerTransportFactory> serverTransportFactory =
      RSocketServerTransportFactory.websocket();
  private Function<LoopResources, RSocketClientTransportFactory> clientTransportFactory =
      RSocketClientTransportFactory.websocket();

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
    this.dataCodecs = other.dataCodecs;
    this.credentialsSupplier = other.credentialsSupplier;
    this.authenticator = other.authenticator;
    this.eventLoopGroup = other.eventLoopGroup;
    this.clientLoopResources = other.clientLoopResources;
    this.serverLoopResources = other.serverLoopResources;
    this.serverTransportFactory = other.serverTransportFactory;
    this.clientTransportFactory = other.clientTransportFactory;
    this.allowedRoles = other.allowedRoles;
  }

  /**
   * Setter for {@code numOfWorkers}.
   *
   * @param numOfWorkers number of worker threads
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport numOfWorkers(int numOfWorkers) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.numOfWorkers = numOfWorkers;
    return rst;
  }

  /**
   * Setter for {@code headersCodec}.
   *
   * @param headersCodec headers codec
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport headersCodec(HeadersCodec headersCodec) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.headersCodec = headersCodec;
    return rst;
  }

  /**
   * Setter for {@code dataCodecs}.
   *
   * @param dataCodecs set of data codecs
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport dataCodecs(Collection<DataCodec> dataCodecs) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.dataCodecs = dataCodecs;
    return rst;
  }

  /**
   * Setter for {@code credentialsSupplier}.
   *
   * @param credentialsSupplier credentialsSupplier
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport credentialsSupplier(CredentialsSupplier credentialsSupplier) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.credentialsSupplier = credentialsSupplier;
    return rst;
  }

  /**
   * Setter for {@code authenticator}.
   *
   * @param authenticator authenticator
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport authenticator(Authenticator authenticator) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.authenticator = authenticator;
    return rst;
  }

  /**
   * Setter for {@code serverTransportFactory}.
   *
   * @param serverTransportFactory serverTransportFactory
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport serverTransportFactory(
      Function<LoopResources, RSocketServerTransportFactory> serverTransportFactory) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.serverTransportFactory = serverTransportFactory;
    return rst;
  }

  /**
   * Setter for {@code clientTransportFactory}.
   *
   * @param clientTransportFactory clientTransportFactory
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport clientTransportFactory(
      Function<LoopResources, RSocketClientTransportFactory> clientTransportFactory) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.clientTransportFactory = clientTransportFactory;
    return rst;
  }

  /**
   * Setter for {@code allowedRoles}.
   *
   * @param allowedRoles allowedRoles
   * @return new {@link RSocketServiceTransport} instance
   */
  public RSocketServiceTransport allowedRoles(List<String> allowedRoles) {
    RSocketServiceTransport rst = new RSocketServiceTransport(this);
    rst.allowedRoles = allowedRoles;
    return rst;
  }

  @Override
  public ClientTransport clientTransport() {
    return new RSocketClientTransport(
        headersCodec,
        dataCodecs,
        clientTransportFactory.apply(clientLoopResources),
        credentialsSupplier,
        allowedRoles);
  }

  @Override
  public ServerTransport serverTransport(ServiceRegistry serviceRegistry) {
    return new RSocketServerTransport(
        authenticator,
        serviceRegistry,
        headersCodec,
        dataCodecs,
        serverTransportFactory.apply(serverLoopResources));
  }

  @Override
  public ServiceTransport start() {
    eventLoopGroup = newEventLoopGroup();
    clientLoopResources = DelegatedLoopResources.newClientLoopResources(eventLoopGroup);
    serverLoopResources = DelegatedLoopResources.newServerLoopResources(eventLoopGroup);
    return this;
  }

  @Override
  public void stop() {
    if (serverLoopResources != null && eventLoopGroup != null) {
      serverLoopResources.dispose();
      eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
    }
  }

  private EventLoopGroup newEventLoopGroup() {
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);
    EventLoopGroup eventLoopGroup =
        Epoll.isAvailable()
            ? new EpollEventLoopGroup(numOfWorkers, threadFactory)
            : new NioEventLoopGroup(numOfWorkers, threadFactory);
    return LoopResources.colocate(eventLoopGroup);
  }
}
