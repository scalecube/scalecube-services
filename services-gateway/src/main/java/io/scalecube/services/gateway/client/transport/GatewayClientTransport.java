package io.scalecube.services.gateway.client.transport;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.gateway.client.GatewayClient;
import io.scalecube.services.gateway.client.GatewayClientChannel;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;

public class GatewayClientTransport implements ClientTransport {

  private final GatewayClient gatewayClient;

  public GatewayClientTransport(GatewayClient gatewayClient) {
    this.gatewayClient = gatewayClient;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    return new GatewayClientChannel(gatewayClient);
  }
}
