package io.scalecube.services.gateway.websocket;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.AbstractLocalGatewayExtension;
import io.scalecube.services.gateway.AuthRegistry;
import io.scalecube.services.gateway.GatewaySessionHandlerImpl;
import io.scalecube.services.gateway.client.transport.GatewayClientTransports;
import io.scalecube.services.gateway.ws.WebsocketGateway;

public class WebsocketLocalWithAuthExtension extends AbstractLocalGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "ws";

  WebsocketLocalWithAuthExtension(Object serviceInstance, AuthRegistry authReg) {
    this(ServiceInfo.fromServiceInstance(serviceInstance).build(), authReg);
  }

  WebsocketLocalWithAuthExtension(ServiceInfo serviceInfo, AuthRegistry authReg) {
    super(
        serviceInfo,
        opts ->
            new WebsocketGateway(
                opts.id(GATEWAY_ALIAS_NAME), new GatewaySessionHandlerImpl(authReg)),
        GatewayClientTransports::websocketGatewayClientTransport);
  }
}
