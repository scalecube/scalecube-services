package io.scalecube.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.AbstractGatewayExtension;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.codec.WebsocketGatewayMessageCodec;
import io.scalecube.gateway.clientsdk.websocket.WebsocketClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.gateway.GatewayConfig;
import reactor.ipc.netty.resources.LoopResources;

class WebsocketGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "ws";

  WebsocketGatewayExtension(Object serviceInstance) {
    super(
        serviceInstance, GatewayConfig.builder(GATEWAY_ALIAS_NAME, WebsocketGateway.class).build());
  }

  @Override
  protected ClientTransport transport() {
    return new WebsocketClientTransport(
        clientSettings(), clientMessageCodec(), LoopResources.create(gatewayAliasName() + "-loop"));
  }

  @Override
  protected ClientMessageCodec<ByteBuf> clientMessageCodec() {
    return new WebsocketGatewayMessageCodec(DataCodec.getInstance(clientSettings().contentType()));
  }

  @Override
  protected String gatewayAliasName() {
    return GATEWAY_ALIAS_NAME;
  }
}
