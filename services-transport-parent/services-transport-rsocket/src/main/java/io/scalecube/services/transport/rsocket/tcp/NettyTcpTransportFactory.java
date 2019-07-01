package io.scalecube.services.transport.rsocket.tcp;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.net.Address;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServerTransportFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/** Create low-level transport for RSocket based on Netty Tcp. */
public class NettyTcpTransportFactory
    implements RSocketClientTransportFactory, RSocketServerTransportFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpTransportFactory.class);

  private final TcpClient tcpClient;
  private final TcpServer tcpServer;

  /**
   * Creates instance from external tcp client&server template.
   *
   * @param tcpClient tcp client template.
   * @param tcpServer tcp server template.
   */
  public NettyTcpTransportFactory(TcpClient tcpClient, TcpServer tcpServer) {
    this.tcpClient = tcpClient;
    this.tcpServer = tcpServer;
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
   * @param address address service
   * @return RSocket Server Transport
   */
  @Override
  public ServerTransport<Server> createServerTransport(Address address) {
    TcpServer tcpServer = this.tcpServer.host(address.host()).port(address.port());

    return new NettyServerTransportAdapter(tcpServer);
  }

  /**
   * Adapter CloseableChannel => Server.
   *
   * @see io.scalecube.services.transport.rsocket.RSocketServerTransportFactory.Server
   */
  private static class NettyServerTransportAdapter implements ServerTransport<Server> {

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

  private static class NettyServer implements Server {

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
      Mono<Void> closeConnections =
          Mono.whenDelayError(
                  connections.stream()
                      .map(
                          connection -> {
                            connection.dispose();
                            return connection
                                .onTerminate()
                                .doOnError(e -> LOGGER.warn("Failed to close connection: " + e));
                          })
                      .collect(Collectors.toList()))
              .doOnTerminate(connections::clear);
      return closeConnections.then(delegate.onClose());
    }

    @Override
    public void dispose() {
      delegate.dispose();
    }
  }
}
