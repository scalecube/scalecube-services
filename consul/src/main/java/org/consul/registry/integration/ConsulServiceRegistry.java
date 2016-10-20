package org.consul.registry.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.Service;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.RemoteServiceInstance;
import io.scalecube.services.ServiceDiscovery;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;

public class ConsulServiceRegistry implements ServiceDiscovery{
  final Consul consul;

  final AgentClient agentClient;

  private ICluster cluster;

  
  public ConsulServiceRegistry() {
    consul = Consul.builder()
        //.withHostAndPort(HostAndPort.fromString(AppConfig.consulAddress()))
        .build();
    agentClient = consul.agentClient();
    this.cluster = cluster;
  }

  public void registerService(ServiceRegistration registration) {

    Registration reg = ImmutableRegistration.builder()
        .address(registration.ip())
        .port(registration.port())
        .name(registration.serviceName())
        .id(registration.memberId())
        .build();

    // register new service
    agentClient.register(reg);
  }

  public List<RemoteServiceInstance> getRemoteServices() {
    List<RemoteServiceInstance> services = new ArrayList();
    for (Map.Entry<String, Service> service : consul.agentClient().getServices().entrySet()) {
      if (!service.getValue().getId().equals(cluster.member().id()) &&
          !service.getValue().getId().equals("consul")) {
        try {
          services.add(new RemoteServiceInstance(cluster,
              new ServiceReference(service.getValue().getId(), service.getValue().getService())));
        } catch (RuntimeException ex) {
          // service reference no longer valid or not reachable
          consul.agentClient().deregister(service.getValue().getId());
        }
      }
    }
    return services;
  }
  
  public List<RemoteServiceInstance> serviceLookup(String serviceName) {
    List<RemoteServiceInstance> services = new ArrayList();
    for (Map.Entry<String, Service> service : consul.agentClient().getServices().entrySet()) {
      if (service.getValue().getService().equals(serviceName)) {
        if(!service.getValue().getId().equals(cluster.member().id()))
          services.add(new RemoteServiceInstance(cluster,
                new ServiceReference(service.getValue().getId(), serviceName)));
      }
    }
    return services;
  }

  @Override
  public void cluster(ICluster cluster) {
    this.cluster = cluster;
  }
}
