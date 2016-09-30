package io.scalecube.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.Service;

import io.scalecube.cluster.ICluster;

public class ConsulServiceRegistry {
  final Consul consul;

  final AgentClient agentClient;

  private ICluster cluster;

  public ConsulServiceRegistry(ICluster cluster , String consulIp) {
    consul = Consul.builder()

        .build();
    agentClient = consul.agentClient();
    this.cluster = cluster;
  }

  public void registerService(String memberId, String serviceName, String ip, int port) {

    Registration reg = ImmutableRegistration.builder()
        .address(ip)
        .port(port)
        .name(serviceName)
        .id(memberId)
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
}
