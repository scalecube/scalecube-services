package io.scalecube.services.gateway.http;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.AbstractGatewayExtension;
import io.scalecube.services.gateway.transport.GatewayClientTransports;

class HttpGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "http";

  HttpGatewayExtension(Object serviceInstance) {
    this(ServiceInfo.fromServiceInstance(serviceInstance).build());
  }

  HttpGatewayExtension(ServiceInfo serviceInfo) {
    super(
        serviceInfo,
        opts -> new HttpGateway(opts.id(GATEWAY_ALIAS_NAME)),
        GatewayClientTransports::httpGatewayClientTransport);
  }
}
