package io.scalecube.services;

import java.util.Collection;

public interface MicroservicesMBean {

  Collection<String> getId();

  Collection<String> getDiscoveryAddress();

  Collection<String> getGatewayAddresses();

  Collection<String> getServiceEndpoint();

  Collection<String> getServiceEndpoints();

  Collection<String> getRecentServiceDiscoveryEvents();

  Collection<String> getClientServiceTransport();

  Collection<String> getServerServiceTransport();

  Collection<String> getServiceDiscovery();
}
