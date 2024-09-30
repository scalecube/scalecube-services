package io.scalecube.services.gateway.websocket;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.AbstractGatewayExtension;
import io.scalecube.services.gateway.client.GatewayClientTransports;

class WebsocketGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "ws";

  WebsocketGatewayExtension(Object serviceInstance) {
    this(ServiceInfo.fromServiceInstance(serviceInstance).build());
  }

  WebsocketGatewayExtension(ServiceInfo serviceInfo) {
    super(
        serviceInfo,
        opts -> new WebsocketGateway.Builder().options(opts.id(GATEWAY_ALIAS_NAME)).build(),
        GatewayClientTransports::websocketGatewayClientTransport);
  }
}
