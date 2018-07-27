package io.scalecube.services.discovery;

import io.scalecube.Throwables;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private static final ObjectMapper objectMapper = newObjectMapper();
  public static final String SERVICE_METADATA = "service";

  private final ServiceRegistry serviceRegistry;

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED;
  }

  public ServiceDiscovery(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public void init(Cluster cluster) {
    loadClusterServices(cluster);
    listenCluster(cluster);
  }

  private void listenCluster(Cluster cluster) {
    cluster.listenMembership().subscribe(event -> {
      if (event.isAdded()) {
        loadMemberServices(DiscoveryType.ADDED, event.member());
      } else if (event.isRemoved()) {
        loadMemberServices(DiscoveryType.REMOVED, event.member());
      }
    });
  }

  private void loadClusterServices(Cluster cluster) {
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
          if ((type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED))
              && (serviceRegistry.registerService(serviceEndpoint))) {

            LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}",
                member, serviceEndpoint);

          } else if (type.equals(DiscoveryType.REMOVED)
              && (serviceRegistry.unregisterService(serviceEndpoint.id()) != null)) {

            LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}",
                member, serviceEndpoint);
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

  public static ServiceEndpoint decodeMetadata(String metadata) {
    try {
      return objectMapper.readValue(metadata, ServiceEndpoint.class);
    } catch (IOException e) {
      LOGGER.error("Can read metadata: " + e, e);
      return null;
    }
  }

  public static String encodeMetadata(ServiceEndpoint serviceEndpoint) {
    try {
      return objectMapper.writeValueAsString(serviceEndpoint);
    } catch (IOException e) {
      LOGGER.error("Can write metadata: " + e, e);
      throw Throwables.propagate(e);
    }
  }
}
