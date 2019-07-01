package io.scalecube.services.transport.rsocket.tcp;

import static java.util.stream.Collectors.toList;

import io.netty.bootstrap.ServerBootstrapConfig;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.net.Address;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServerTransportFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import reactor.util.annotation.Nullable;

/** Create low-level transport for RSocket based on Netty Tcp. */
public class NettyTcpTransportFactory
    implements RSocketClientTransportFactory, RSocketServerTransportFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpTransportFactory.class);

  private final TcpClient tcpClient;
  private final ServerTransport<Server> serverTransport;

  @Nullable private LoopResources clientResources;
  @Nullable private LoopResources serverResources;

  /**
   * Creates instance from external tcp client/server.
   *
   * @param tcpClient tcp client.
   * @param tcpServer tcp server.
   */
  public NettyTcpTransportFactory(TcpClient tcpClient, TcpServer tcpServer) {
    this.tcpClient = tcpClient;
    this.serverTransport = new NettyServerTransportAdapter(tcpServer);
  }

  /**
   * Creates instance tcp client/server and customizes them.
   *
   * @param userClientCustomizers client customizers
   * @param userServerCustomizers server customizers
   * @param clientResourceFactory client default loop resource factory
   * @param serverResourceFactory server default loop resource factory
   */
  public NettyTcpTransportFactory(
      List<Function<TcpClient, TcpClient>> userClientCustomizers,
      List<Function<TcpServer, TcpServer>> userServerCustomizers,
      Supplier<LoopResources> clientResourceFactory,
      Supplier<LoopResources> serverResourceFactory) {

    this.tcpClient = createAndCustomizeClient(userClientCustomizers, clientResourceFactory);
    TcpServer tcpServer = createAndCustomizeServer(userServerCustomizers, serverResourceFactory);
    this.serverTransport = new NettyServerTransportAdapter(tcpServer);
  }

  /**
   * Initialize the tcp client template by host&port of remote service.
   *
   * @param address address remote service
   * @return RSocket Client Transport
   */
  @Override
  public ClientTransport createClient(Address address) {
    TcpClient tcpClient = this.tcpClient.host(address.host()).port(address.port());
    return TcpClientTransport.create(tcpClient);
  }

  /**
   * Initialize the tcp server template by host&port of service.
   *
   * @return RSocket Server Transport
   */
  @Override
  public ServerTransport<Server> createServerTransport() {
    return this.serverTransport;
  }

  private TcpServer createAndCustomizeServer(
      List<Function<TcpServer, TcpServer>> userServerCustomizers,
      Supplier<LoopResources> serverResourceFactory) {
    Function<TcpServer, TcpServer> defaultCustomizer =
        server -> {
          TcpServer temp = server;
          ServerBootstrapConfig serverConfig = server.configure().config();
          if (serverConfig.group() == null) {
            this.serverResources = serverResourceFactory.get();
            temp = temp.runOn(this.serverResources, true);
          }
          if (serverConfig.localAddress() == null) {
            temp = temp.port(0);
          }
          return temp;
        };
    Function<TcpServer, TcpServer> reducedServerCustomizer =
        userServerCustomizers.stream().reduce(Function::andThen).orElse(Function.identity());
    return reducedServerCustomizer.andThen(defaultCustomizer).apply(TcpServer.create());
  }

  private TcpClient createAndCustomizeClient(
      List<Function<TcpClient, TcpClient>> userClientCustomizers,
      Supplier<LoopResources> clientResourceFactory) {
    Function<TcpClient, TcpClient> defaultCustomizer =
        client -> {
          boolean resourceNotExists = client.configure().config().group() == null;
          if (resourceNotExists) {
            this.clientResources = clientResourceFactory.get();
            return client.runOn(this.clientResources, true);
          }
          return client;
        };
    Function<TcpClient, TcpClient> reducedClientCustomizer =
        userClientCustomizers.stream().reduce(Function::andThen).orElse(Function.identity());

    return reducedClientCustomizer.andThen(defaultCustomizer).apply(TcpClient.newConnection());
  }

  /**
   * Adapter CloseableChannel => Server.
   *
   * @see io.scalecube.services.transport.rsocket.RSocketServerTransportFactory.Server
   */
  private class NettyServerTransportAdapter implements ServerTransport<Server> {

    private final ServerTransport<CloseableChannel> delegate;

    private final List<Connection> connections;

    private NettyServerTransportAdapter(TcpServer server) {
      this.connections = new CopyOnWriteArrayList<>();
      TcpServer tcpServer =
          server.doOnConnection(
              connection -> {
                LOGGER.info("Accepted connection on {}", connection.channel());
                connection.onDispose(
                    () -> {
                      LOGGER.info("Connection closed on {}", connection.channel());
                      connections.remove(connection);
                    });
                connections.add(connection);
              });
      this.delegate = TcpServerTransport.create(tcpServer);
    }

    @Override
    public Mono<Server> start(ConnectionAcceptor acceptor) {
      return delegate.start(acceptor).map(delegate1 -> new NettyServer(delegate1, connections));
    }
  }

  private class NettyServer implements Server {

    private final CloseableChannel delegate;
    private final List<Connection> connections;

    private NettyServer(CloseableChannel delegate, List<Connection> connections) {
      this.delegate = delegate;
      this.connections = connections;
    }

    @Override
    public Address address() {
      InetSocketAddress address = delegate.address();
      return Address.create(address.getHostString(), address.getPort());
    }

    @Override
    public Mono<Void> onClose() {
      List<Mono<Void>> disposables = new ArrayList<>(disposeConnections());
      if (clientResources != null) {
        disposables.add(clientResources.disposeLater());
      }
      if (serverResources != null) {
        disposables.add(serverResources.disposeLater());
      }
      disposables.add(delegate.onClose());
      return Mono.whenDelayError(disposables).doOnTerminate(connections::clear);
    }

    private List<Mono<Void>> disposeConnections() {
      return connections.stream()
          .map(
              connection -> {
                connection.dispose();
                return connection
                    .onTerminate()
                    .doOnError(e -> LOGGER.warn("Failed to close connection: " + e));
              })
          .collect(toList());
    }

    @Override
    public void dispose() {
      delegate.dispose();
    }
  }

  public NettyTcpTransportFactory setClientResources(@Nullable LoopResources clientResources) {
    this.clientResources = clientResources;
    return this;
  }

  public NettyTcpTransportFactory setServerResources(@Nullable LoopResources serverResources) {
    this.serverResources = serverResources;
    return this;
  }
}
