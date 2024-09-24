package io.scalecube.services.transport.rsocket;

import io.netty.channel.ChannelOption;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.scalecube.services.Address;
import java.util.function.Function;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
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
    return tcp(false);
  }

  /**
   * Returns default rsocket tcp client transport factory.
   *
   * @see TcpClientTransport
   * @param isSecured is client transport secured
   * @return factory function for {@link RSocketClientTransportFactory}
   */
  static Function<LoopResources, RSocketClientTransportFactory> tcp(boolean isSecured) {
    return (LoopResources loopResources) ->
        (RSocketClientTransportFactory)
            address -> {
              TcpClient tcpClient =
                  TcpClient.newConnection()
                      .runOn(loopResources)
                      .host(address.host())
                      .port(address.port())
                      .option(ChannelOption.TCP_NODELAY, true)
                      .option(ChannelOption.SO_KEEPALIVE, true)
                      .option(ChannelOption.SO_REUSEADDR, true);
              return TcpClientTransport.create(isSecured ? tcpClient.secure() : tcpClient);
            };
  }

  /**
   * Returns default rsocket websocket client transport factory.
   *
   * @see WebsocketClientTransport
   * @return factory function for {@link RSocketClientTransportFactory}
   */
  static Function<LoopResources, RSocketClientTransportFactory> websocket() {
    return websocket(false);
  }

  /**
   * Returns default rsocket websocket client transport factory.
   *
   * @see WebsocketClientTransport
   * @param isSecured is client transport secured
   * @return factory function for {@link RSocketClientTransportFactory}
   */
  static Function<LoopResources, RSocketClientTransportFactory> websocket(boolean isSecured) {
    return (LoopResources loopResources) ->
        (RSocketClientTransportFactory)
            address -> {
              HttpClient httpClient =
                  HttpClient.create(ConnectionProvider.newConnection())
                      .runOn(loopResources)
                      .host(address.host())
                      .port(address.port())
                      .option(ChannelOption.TCP_NODELAY, true)
                      .option(ChannelOption.SO_KEEPALIVE, true)
                      .option(ChannelOption.SO_REUSEADDR, true);

              httpClient = isSecured ? httpClient.secure() : httpClient;

              return WebsocketClientTransport.create(httpClient, "/");
            };
  }

  ClientTransport clientTransport(Address address);
}
