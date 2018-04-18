package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.concurrency.ThreadFactory;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ServiceDiscovery {

  private Microservices microservices;

  private final ServiceRegistry serviceRegistry; // service_registry -> cluster (on start on shuytdown)

  private Cluster cluster; // cluster -> service_registry (on listen cluster events)

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }
  
  public ServiceDiscovery(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void start(ClusterConfig.Builder config) {

    Collection<ServiceReference> services = serviceRegistry.services(); // -> []
    ClusterConfig cfg = getClusterConfig(config, services).build();
    this.cluster = Cluster.joinAwait(cfg); 
    loadClusterServices();
    listenCluster();
  }

  /**
   * Shutdown services transport and cluster transport.
   *
   * @return future with cluster shutdown result.
   */
  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> result = new CompletableFuture<Void>();

    if (!this.cluster.isShutdown()) {
      return this.cluster.shutdown();
    } else {
      result.completeExceptionally(new IllegalStateException("Cluster transport alredy stopped"));
      return result;
    }
  }
  
  private ClusterConfig.Builder getClusterConfig(
      ClusterConfig.Builder clusterConfig, 
      Collection<ServiceReference> services) {
    
    if (services != null && !services.isEmpty()) {
      clusterConfig.addMetadata(metadata(services));
    }
    return clusterConfig;
  }
  private static Map<String, String> metadata(Collection<ServiceReference> services) {
    Map<String, String> servicesTags = new HashMap<>();

    services.stream().forEach(service -> {
      // constract info metadata object as json key
      ServiceInfo info = new ServiceInfo(
          service.serviceName(),
          service.methods(),
          service.tags()
          );
      
      // add service metadata to map.
      servicesTags.put(info.toMetadata(), "service");
    });

    return servicesTags;
  }
  
  private void listenCluster() {
    cluster.listenMembership().subscribe(event -> {
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
    cluster.otherMembers().forEach(member -> {
      loadMemberServices(DiscoveryType.DISCOVERED, member);
    });
  }
  
  private void loadMemberServices(DiscoveryType type, Member member) {
    member.metadata().entrySet().stream()
        .filter(entry -> "service".equals(entry.getValue())) // filter service tags
        .forEach(entry -> {
          Address serviceAddress = getServiceAddress(member);
          ServiceInfo info = ServiceInfo.from(entry.getKey());
          
          LOGGER.debug("Member: {} is {} : {}", member, type, serviceRef);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
            if (!containsService(info)) {
              serviceRegistry.registerService(new ServiceInstance( , info.getTags()));
              LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}", member,
                  serviceRef);
            }
          } else if (type.equals(DiscoveryType.REMOVED)) {
            serviceRegistry.remove(serviceRef);
            LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}", member,
                serviceRef);
          }
        });
  }

  private Address getServiceAddress(Member member) {
    String serviceAddressAsString = member.metadata().get("service-address");
    if (serviceAddressAsString != null) {
      return Address.from(serviceAddressAsString);
    } else {
      return member.address();
    }
  }
}
