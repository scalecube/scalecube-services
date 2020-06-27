package io.scalecube.services.transport.rsocket;

import io.netty.channel.ChannelOption;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.scalecube.net.Address;
import java.util.function.Function;
import reactor.netty.http.client.HttpClient;
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

  /**
   * Returns default rsocket websocket client transport factory.
   *
   * @see WebsocketClientTransport
   * @return factory function for {@link RSocketClientTransportFactory}
   */
  static Function<LoopResources, RSocketClientTransportFactory> websocket() {
    return (LoopResources loopResources) ->
        (RSocketClientTransportFactory)
            address ->
                WebsocketClientTransport.create(
                    HttpClient.newConnection()
                        .tcpConfiguration(
                            tcpClient ->
                                tcpClient
                                    .runOn(loopResources)
                                    .host(address.host())
                                    .port(address.port())
                                    .option(ChannelOption.TCP_NODELAY, true)
                                    .option(ChannelOption.SO_KEEPALIVE, true)
                                    .option(ChannelOption.SO_REUSEADDR, true)),
                    "/");
  }

  ClientTransport clientTransport(Address address);
}
