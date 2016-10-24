package io.scalecube.services;

import java.util.List;

import io.scalecube.cluster.ICluster;

public interface ServiceDiscovery {

  void registerService(ServiceRegistration registration);

  List<RemoteServiceInstance> getRemoteServices();

  List<RemoteServiceInstance> serviceLookup(String serviceName);

  void cluster(ICluster cluster);

  void cleanup();

}
