package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import java.util.function.Function;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public interface RSocketServerTransportFactory {

  /**
   * Returns default rsocket tcp server transport factory (shall listen on port {@code 0}).
   *
   * @see TcpServerTransport
   * @return factory function for {@link RSocketServerTransportFactory}
   */
  static Function<LoopResources, RSocketServerTransportFactory> tcp() {
    return tcp(0);
  }

  /**
   * Returns default rsocket tcp server transport factory.
   *
   * @param port port
   * @see TcpServerTransport
   * @return factory function for {@link RSocketServerTransportFactory}
   */
  static Function<LoopResources, RSocketServerTransportFactory> tcp(int port) {
    return (LoopResources loopResources) ->
        () ->
            TcpServerTransport.create(
                TcpServer.create()
                    .runOn(loopResources)
                    .bindAddress(() -> new InetSocketAddress(port)));
  }

  ServerTransport<CloseableChannel> serverTransport();
}
