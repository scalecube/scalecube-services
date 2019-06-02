package io.scalecube.services.gateway.rsocket;

import io.rsocket.Payload;
import io.scalecube.services.gateway.AbstractGatewayExtension;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import reactor.netty.resources.LoopResources;

class RsocketGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "rsws";

  RsocketGatewayExtension(Object serviceInstance) {
    super(serviceInstance, GatewayConfig.builder(GATEWAY_ALIAS_NAME, RSocketGateway.class).build());
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
