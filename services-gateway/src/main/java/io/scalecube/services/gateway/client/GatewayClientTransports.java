package io.scalecube.services.gateway.client;

import io.scalecube.services.gateway.client.http.HttpGatewayClient;
import io.scalecube.services.gateway.client.http.HttpGatewayClientCodec;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClient;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientCodec;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import java.util.function.Function;

public class GatewayClientTransports {

  public static final WebsocketGatewayClientCodec WEBSOCKET_CLIENT_CODEC =
      new WebsocketGatewayClientCodec();

  private GatewayClientTransports() {
    // utils
  }

  /**
   * ClientTransport that is capable of communicating with Gateway over websocket.
   *
   * @param cs client settings for gateway client transport
   * @return client transport
   */
  public static ClientTransport websocketGatewayClientTransport(GatewayClientSettings cs) {
    final Function<GatewayClientSettings, GatewayClient> function =
        settings -> new WebsocketGatewayClient(settings, WEBSOCKET_CLIENT_CODEC);
    return new GatewayClientTransport(function.apply(cs));
  }
}
