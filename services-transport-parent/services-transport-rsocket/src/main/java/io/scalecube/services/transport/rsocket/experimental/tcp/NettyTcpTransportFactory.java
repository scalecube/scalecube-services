package io.scalecube.services.transport.rsocket.experimental.tcp;

import io.rsocket.Closeable;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.scalecube.net.Address;
import io.scalecube.services.transport.rsocket.experimental.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.experimental.RSocketServerTransportFactory;
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
  public ServerTransport<? extends Closeable> createServer(Address address) {
    TcpServer tcpServer = this.tcpServer.host(address.host()).port(address.port());
    return TcpServerTransport.create(tcpServer);
  }
}
