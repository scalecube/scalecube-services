package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.ClusterMetadataDecoder.decodeMetadata;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterConfig.Builder;
import io.scalecube.cluster.Member;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.DiscoveryConfig;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  public static final String SERVICE_METADATA = "service";

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private ServiceRegistry serviceRegistry;

  private Cluster cluster;

  private ServiceEndpoint endpoint;

  private enum DiscoveryType {
    ADDED,
    REMOVED,
    DISCOVERED;
  }

  @Override
  public Address address() {
    return cluster.address();
  }

  @Override
  public ServiceEndpoint endpoint() {
    return this.endpoint;
  }

  @Override
  public Mono<ServiceDiscovery> start(DiscoveryConfig discoveryConfig) {
    this.serviceRegistry = discoveryConfig.serviceRegistry();
    this.endpoint = discoveryConfig.endpoint();

    ClusterConfig clusterConfig =
        clusterConfigBuilder(discoveryConfig)
            .addMetadata(
                this.serviceRegistry
                    .listServiceEndpoints()
                    .stream()
                    .collect(
                        Collectors.toMap(
                            ClusterMetadataDecoder::encodeMetadata, service -> SERVICE_METADATA)))
            .build();

    LOGGER.info("Start scalecube service discovery with config: {}", clusterConfig);

    CompletableFuture<Cluster> promise =
        Cluster.join(clusterConfig)
            .whenComplete(
                (success, error) -> {
                  if (error == null) {
                    this.cluster = success;
                    this.init(this.cluster);
                  }
                });

    return Mono.fromFuture(promise).map(mapper -> this);
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> Optional.ofNullable(cluster)
            .map(cluster1 -> Mono.fromFuture(cluster1.shutdown()))
            .orElse(Mono.empty()));
  }

  private ClusterConfig.Builder clusterConfigBuilder(DiscoveryConfig config) {
    Builder builder = ClusterConfig.builder();
    if (config.seeds() != null) {
      builder.seedMembers(config.seeds());
    }
    if (config.port() != null) {
      builder.port(config.port());
    }
    if (config.tags() != null) {
      builder.metadata(config.tags());
    }
    if (config.memberHost() != null) {
      builder.memberHost(config.memberHost());
    }
    if (config.memberPort() != null) {
      builder.memberPort(config.memberPort());
    }
    return builder;
  }

  private void init(Cluster cluster) {
    loadClusterServices(cluster);
    listenCluster(cluster);
  }

  private void listenCluster(Cluster cluster) {
    cluster
        .listenMembership()
        .subscribe(
            event -> {
              if (event.isAdded()) {
                loadMemberServices(DiscoveryType.ADDED, event.member());
              } else if (event.isRemoved()) {
                loadMemberServices(DiscoveryType.REMOVED, event.member());
              }
            });
  }

  private void loadClusterServices(Cluster cluster) {
    cluster
        .otherMembers()
        .forEach(
            member -> loadMemberServices(DiscoveryType.DISCOVERED, member));
  }

  private void loadMemberServices(DiscoveryType type, Member member) {
    member
        .metadata()
        .entrySet()
        .stream()
        .filter(entry -> SERVICE_METADATA.equals(entry.getValue()))
        .forEach(
            entry -> {
              ServiceEndpoint serviceEndpoint = decodeMetadata(entry.getKey());
              if (serviceEndpoint == null) {
                return;
              }

              LOGGER.debug("Member: {} is {} : {}", member, type, serviceEndpoint);
              if ((type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED))
                  && (this.serviceRegistry.registerService(serviceEndpoint))) {
                LOGGER.info(
                    "Service Reference was ADDED since new Member has joined the cluster {} : {}",
                    member,
                    serviceEndpoint);
              } else if (type.equals(DiscoveryType.REMOVED)
                  && (this.serviceRegistry.unregisterService(serviceEndpoint.id()) != null)) {
                LOGGER.info(
                    "Service Reference was REMOVED since Member have left the cluster {} : {}",
                    member,
                    serviceEndpoint);
              }
            });
  }
}
