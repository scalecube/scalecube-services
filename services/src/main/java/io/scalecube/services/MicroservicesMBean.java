package io.scalecube.services;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public interface MicroservicesMBean {

  String getId();

  InetSocketAddress getServiceAddress();

  String getDiscoveryAddress();

  List<String> getDiscoverySeeds();

  Map<String, String> getDiscoveryTags();

  String getDiscoveryMemberHost();

  Integer getDiscoveryMemberPort();

  Map<String, InetSocketAddress> gatewayAddresses();

  String serviceEndpoint();

  List<String> last30ServiceDiscoveryEvents();

  List<String> serviceEndpoints();

  List<String> serviceReferences();

  String serviceTransport();

  String serviceDiscovery();
}
