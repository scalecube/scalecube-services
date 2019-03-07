package io.scalecube.services.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.gateway.AbstractGatewayExtension;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.gateway.clientsdk.websocket.WebsocketClientCodec;
import io.scalecube.services.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import io.scalecube.services.transport.api.DataCodec;
import reactor.netty.resources.LoopResources;

class WebsocketGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "ws";

  WebsocketGatewayExtension(Object serviceInstance) {
    super(serviceInstance, opts -> new WebsocketGateway(opts.id(GATEWAY_ALIAS_NAME)));
  }

  @Override
  protected ClientTransport transport() {
    return new WebsocketClientTransport(
        clientSettings(), clientMessageCodec(), LoopResources.create(gatewayAliasName() + "-loop"));
  }

  @Override
  protected ClientCodec<ByteBuf> clientMessageCodec() {
    return new WebsocketClientCodec(DataCodec.getInstance(clientSettings().contentType()));
  }

  @Override
  protected String gatewayAliasName() {
    return GATEWAY_ALIAS_NAME;
  }
}
