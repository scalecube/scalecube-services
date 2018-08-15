package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.AbstractGatewayExtention;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.services.gateway.GatewayConfig;
import reactor.ipc.netty.resources.LoopResources;

public class RsocketGatewayExtension extends AbstractGatewayExtention {

  public RsocketGatewayExtension(Object serviceInstance) {
    super(serviceInstance, GatewayConfig.builder(RSocketWebsocketGateway.class).build());
  }

  @Override
  protected RSocketClientTransport transport(ClientSettings settings, ClientMessageCodec codec) {
    return new RSocketClientTransport(settings, codec, LoopResources.create("eventLoop"));
  }
}
