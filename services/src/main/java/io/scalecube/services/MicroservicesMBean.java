package io.scalecube.services;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public interface MicroservicesMBean {

  String getId();

  String getDiscoveryAddress();

  Map<String, InetSocketAddress> getGatewayAddresses();

  String getServiceEndpoint();

  List<String> getRecentServiceDiscoveryEvents();

  List<String> getServiceEndpoints();

  List<String> getServiceReferences();

  String getServiceTransport();

  String getServiceDiscovery();
}
