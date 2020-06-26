package io.scalecube.services.transport.rsocket;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.scalecube.net.Address;
import java.util.function.Function;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public interface RSocketClientTransportFactory {

  /**
   * Returns default rsocket tcp client transport factory.
   *
   * @see TcpClientTransport
   * @return factory function for {@link RSocketClientTransportFactory}
   */
  static Function<LoopResources, RSocketClientTransportFactory> tcp() {
    return (LoopResources loopResources) ->
        (RSocketClientTransportFactory)
            address ->
                TcpClientTransport.create(
                    TcpClient.newConnection()
                        .host(address.host())
                        .port(address.port())
                        .runOn(loopResources));
  }

  ClientTransport clientTransport(Address address);
}
