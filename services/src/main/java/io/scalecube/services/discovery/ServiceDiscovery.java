package io.scalecube.services.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.concurrency.ThreadFactory;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private static final ObjectMapper objectMapper = newObjectMapper();

  private final ServiceRegistry serviceRegistry; // service_registry -> cluster (on start on shuytdown)
  private Cluster cluster; // cluster -> service_registry (on listen cluster events)

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }

  public ServiceDiscovery(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void start(ClusterConfig.Builder config) {
    cluster = Cluster.joinAwait(addMetadata(config, serviceRegistry.listServices()).build());
    loadClusterServices();
    listenCluster();
  }

  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (!cluster.isShutdown()) {
      return cluster.shutdown();
    } else {
      result.completeExceptionally(new IllegalStateException("Cluster transport alredy stopped"));
      return result;
    }
  }

  private ClusterConfig.Builder addMetadata(ClusterConfig.Builder cfg, Collection<ServiceReference> serviceReferences) {
    if (serviceReferences != null && !serviceReferences.isEmpty()) {
      cfg.addMetadata(serviceReferences.stream()
          .collect(Collectors.toMap(ServiceDiscovery::toMetadataString, service -> "service")));
    }
    return cfg;
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
        .filter(entry -> "service".equals(entry.getValue()))
        .forEach(entry -> {
          ServiceReference serviceReference = fromMetadataString(entry.getKey());

          LOGGER.debug("Member: {} is {} : {}", member, type, serviceReference);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {

            serviceRegistry.registerService(serviceReference);
            LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}",
                member, serviceReference);
          } else if (type.equals(DiscoveryType.REMOVED)) {

            serviceRegistry.unregisterService(serviceReference);
            LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}",
                member, serviceReference);
          }
        });
  }

  private static ObjectMapper newObjectMapper() {
    ObjectMapper json = new ObjectMapper();
    json.setVisibility(json.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    json.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return json;
  }

  private static ServiceReference fromMetadataString(String metadata) {
    try {
      return objectMapper.readValue(metadata, ServiceReference.class);
    } catch (IOException e) {
      LOGGER.error("Can read metadata: " + e);
      return null;
    }
  }

  private static String toMetadataString(ServiceReference serviceReference) {
    try {
      return objectMapper.writeValueAsString(serviceReference);
    } catch (IOException e) {
      LOGGER.error("Can write metadata: " + e);
      return null;
    }
  }
}
