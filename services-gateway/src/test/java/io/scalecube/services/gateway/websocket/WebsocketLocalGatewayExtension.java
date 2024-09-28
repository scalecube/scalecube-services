package io.scalecube.services.gateway.websocket;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.AbstractLocalGatewayExtension;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.gateway.client.GatewayClientTransports;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import java.util.function.Function;

class WebsocketLocalGatewayExtension extends AbstractLocalGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "ws";

  WebsocketLocalGatewayExtension(Object serviceInstance) {
    this(ServiceInfo.fromServiceInstance(serviceInstance).build());
  }

  WebsocketLocalGatewayExtension(ServiceInfo serviceInfo) {
    this(serviceInfo, WebsocketGateway::new);
  }

  WebsocketLocalGatewayExtension(
      ServiceInfo serviceInfo, Function<GatewayOptions, WebsocketGateway> gatewaySupplier) {
    super(
        serviceInfo,
        opts -> gatewaySupplier.apply(opts.id(GATEWAY_ALIAS_NAME)),
        GatewayClientTransports::websocketGatewayClientTransport);
  }
}
