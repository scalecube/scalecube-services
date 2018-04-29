package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.concurrency.ThreadFactory;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Throwables;

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
  public static final String SERVICE_METADATA = "service";

  private final ServiceRegistry serviceRegistry; // service_registry -> cluster (on start on shuytdown)
  private Cluster cluster; // cluster -> service_registry (on listen cluster events)

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }

  public ServiceDiscovery(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void start(ClusterConfig.Builder config) {
    cluster = Cluster.joinAwait(addMetadata(config, serviceRegistry.listServiceEndpoints()).build());
    loadClusterServices();
    listenCluster();
  }

  public CompletableFuture<Void> shutdown() {
    return cluster.shutdown();
  }

  private ClusterConfig.Builder addMetadata(ClusterConfig.Builder cfg, Collection<ServiceEndpoint> serviceEndpoints) {
    if (serviceEndpoints != null) {
      cfg.addMetadata(serviceEndpoints.stream()
          .collect(Collectors.toMap(ServiceDiscovery::encodeMetadata, service -> SERVICE_METADATA)));
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
        .filter(entry -> SERVICE_METADATA.equals(entry.getValue()))
        .forEach(entry -> {
          ServiceEndpoint serviceEndpoint = decodeMetadata(entry.getKey());
          if (serviceEndpoint == null) {
            return;
          }

          LOGGER.debug("Member: {} is {} : {}", member, type, serviceEndpoint);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
            if (serviceRegistry.registerService(serviceEndpoint)) {
              LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}",
                  member, serviceEndpoint);
            }
          } else if (type.equals(DiscoveryType.REMOVED)) {
            if (serviceRegistry.unregisterService(serviceEndpoint.id()) != null) {
              LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}",
                  member, serviceEndpoint);
            }
          }
        });
  }

  private static ObjectMapper newObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    return objectMapper;
  }

  private static ServiceEndpoint decodeMetadata(String metadata) {
    try {
      return objectMapper.readValue(metadata, ServiceEndpoint.class);
    } catch (IOException e) {
      LOGGER.error("Can read metadata: " + e, e);
      return null;
    }
  }

  private static String encodeMetadata(ServiceEndpoint serviceEndpoint) {
    try {
      return objectMapper.writeValueAsString(serviceEndpoint);
    } catch (IOException e) {
      LOGGER.error("Can write metadata: " + e, e);
      throw Throwables.propagate(e);
    }
  }

  public Cluster cluster() {
    return this.cluster;
  }
}
