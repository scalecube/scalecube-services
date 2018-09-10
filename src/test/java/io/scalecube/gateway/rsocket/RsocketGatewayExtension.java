package io.scalecube.gateway.rsocket;

import io.rsocket.Payload;
import io.scalecube.gateway.AbstractGatewayExtension;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.gateway.GatewayConfig;
import reactor.ipc.netty.resources.LoopResources;

class RsocketGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "rsws";

  RsocketGatewayExtension(Object serviceInstance) {
    super(
        serviceInstance,
        GatewayConfig.builder(GATEWAY_ALIAS_NAME, RSocketWebsocketGateway.class).build());
  }

  @Override
  protected ClientTransport transport() {
    return new RSocketClientTransport(
        clientSettings(), clientMessageCodec(), LoopResources.create(gatewayAliasName() + "-loop"));
  }

  @Override
  protected ClientCodec<Payload> clientMessageCodec() {
    String contentType = clientSettings().contentType();
    HeadersCodec headersCodec = HeadersCodec.getInstance(contentType);
    DataCodec dataCodec = DataCodec.getInstance(contentType);

    return new RSocketClientCodec(headersCodec, dataCodec);
  }

  @Override
  protected String gatewayAliasName() {
    return GATEWAY_ALIAS_NAME;
  }
}
