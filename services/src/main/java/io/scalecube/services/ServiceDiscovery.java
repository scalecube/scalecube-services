package io.scalecube.services;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.concurrency.ThreadFactory;
import io.scalecube.services.registry.ServiceRegistryImpl;

import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ServiceDiscovery {

  private Microservices microservices;

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }
  
  private void listenCluster() {
    microservices.cluster().listenMembership().subscribe(event -> {
      if (event.isAdded()) {
        loadMemberServices(DiscoveryType.ADDED, event.member());
      } else if (event.isRemoved()) {
        loadMemberServices(DiscoveryType.REMOVED, event.member());
      }
    });
    ThreadFactory.singleScheduledExecutorService("service-registry-discovery")
        .scheduleAtFixedRate(this::loadClusterServices, 10, 10, TimeUnit.SECONDS);
  }

  private void loadClusterServices() {
    this.microservices.cluster().otherMembers().forEach(member -> {
      loadMemberServices(DiscoveryType.DISCOVERED, member);
    });
  }
  
  private void loadMemberServices(DiscoveryType type, Member member) {
    member.metadata().entrySet().stream()
        .filter(entry -> "service".equals(entry.getValue())) // filter service tags
        .forEach(entry -> {
          Address serviceAddress = getServiceAddress(member);
          ServiceInfo info = ServiceInfo.from(entry.getKey());
          ServiceReference serviceRef = new ServiceReference(
              member.id(),
              info.getServiceName(),
              info.methods(),
              serviceAddress);

          LOGGER.debug("Member: {} is {} : {}", member, type, serviceRef);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
            if (!serviceInstances.containsKey(serviceRef)) {
              serviceInstances.putIfAbsent(serviceRef,
                  new ServiceInstance(microservices.client(), serviceRef, info.getTags()));
              LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}", member,
                  serviceRef);
            }
          } else if (type.equals(DiscoveryType.REMOVED)) {
            serviceInstances.remove(serviceRef);
            LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}", member,
                serviceRef);
          }
        });
  }

  ClusterConfig cfg = getClusterConfig(clusterConfig, services, serviceAddress).build();
  this.cluster = Cluster.joinAwait(cfg); 

  private ClusterConfig.Builder getClusterConfig(ClusterConfig.Builder clusterConfig, Services services,
      Address address) {
    if (services != null && !services.list().isEmpty()) {
      clusterConfig.addMetadata(Microservices.metadata(services));
      if (address != null) {
        clusterConfig.addMetadata("service-address", address.toString());
      }
    }
    return clusterConfig;
  }

  private Address getServiceAddress(Member member) {
    String serviceAddressAsString = member.metadata().get("service-address");
    if (serviceAddressAsString != null) {
      return Address.from(serviceAddressAsString);
    } else {
      return member.address();
    }
  }

  @Override
  public void start() {
    loadClusterServices();
    listenCluster();
  }

}
