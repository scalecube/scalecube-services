package io.scalecube.services;

import io.scalecube.cluster.ICluster;

import java.util.List;

public interface ServiceDiscovery {

  void registerService(ServiceRegistration registration);

  List<RemoteServiceInstance> getRemoteServices();

  List<RemoteServiceInstance> serviceLookup(String serviceName);

  void cluster(ICluster cluster);

  void cleanup();

}
