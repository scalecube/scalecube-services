package io.scalecube.services.transport.rsocket.experimental.tcp;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.net.Address;
import io.scalecube.services.transport.rsocket.experimental.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.experimental.RSocketServerTransportFactory;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class NettyTcpTransportFactory
    implements RSocketClientTransportFactory, RSocketServerTransportFactory {

  private final TcpClient tcpClient;
  private final TcpServer tcpServer;

  public NettyTcpTransportFactory(TcpClient tcpClient, TcpServer tcpServer) {
    this.tcpClient = tcpClient;
    this.tcpServer = tcpServer;
  }

  @Override
  public ClientTransport createClient(Address address) {
    TcpClient tcpClient = this.tcpClient.host(address.host()).port(address.port());
    return TcpClientTransport.create(tcpClient);
  }

  @Override
  public ServerTransport<Server> createServer(Address address) {
    TcpServer tcpServer = this.tcpServer.host(address.host()).port(address.port());
    ServerTransport<CloseableChannel> serverTransport = TcpServerTransport.create(tcpServer);
    return new NettyServerTransportAdapter(serverTransport);
  }

  private static class NettyServerTransportAdapter implements ServerTransport<Server> {

    private final ServerTransport<CloseableChannel> delegate;

    private NettyServerTransportAdapter(ServerTransport<CloseableChannel> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Mono<Server> start(ConnectionAcceptor acceptor, int mtu) {
      return delegate.start(acceptor, mtu).map(NettyServer::new);
    }
  }

  private static class NettyServer implements Server {

    private final CloseableChannel delegate;

    private NettyServer(CloseableChannel delegate) {
      this.delegate = delegate;
    }

    @Override
    public Address address() {
      InetSocketAddress address = delegate.address();
      return Address.create(address.getHostString(), address.getPort());
    }

    @Override
    public Mono<Void> onClose() {
      return delegate.onClose();
    }

    @Override
    public void dispose() {
      delegate.dispose();
    }
  }
}
