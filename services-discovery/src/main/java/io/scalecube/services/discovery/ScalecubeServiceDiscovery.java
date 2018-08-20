package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.ClusterMetadataDecoder.decodeMetadata;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.DiscoveryConfig;
import io.scalecube.services.discovery.api.DiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  public static final String SERVICE_METADATA = "service";

  private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private ServiceRegistry serviceRegistry;

  private Cluster cluster;

  private ServiceEndpoint endpoint;

  private final DirectProcessor<DiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<DiscoveryEvent> sink = subject.serialize().sink();

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
  public Mono<ServiceDiscovery> start(DiscoveryConfig config) {
    configure(config);

    clusterConfig.addMetadata(
        this.serviceRegistry
            .listServiceEndpoints()
            .stream()
            .collect(
                Collectors.toMap(
                    ClusterMetadataDecoder::encodeMetadata, service -> SERVICE_METADATA)));
    CompletableFuture<Cluster> promise =
        Cluster.join(clusterConfig.build())
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
  public Flux<DiscoveryEvent> listen() {
    return Flux.fromIterable(serviceRegistry.listServiceEndpoints())
        .map(DiscoveryEvent::registered)
        .concatWith(subject);
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(() -> {
      sink.complete();
      return cluster != null ? Mono.fromFuture(cluster.shutdown()) : Mono.empty();
    });
  }

  private void configure(DiscoveryConfig config) {
    this.serviceRegistry = config.serviceRegistry();
    this.endpoint = config.endpoint();

    if (config.seeds() != null) {
      clusterConfig.seedMembers(config.seeds());
    }

    if (config.port() != null) {
      clusterConfig.port(config.port());
    }

    if (config.tags() != null) {
      clusterConfig.metadata(config.tags());
    }
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
            member -> {
              loadMemberServices(DiscoveryType.DISCOVERED, member);
            });
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

                DiscoveryEvent registrationEvent = DiscoveryEvent.registered(serviceEndpoint);
                LOGGER.debug("Publish registered: " + registrationEvent);
                sink.next(registrationEvent);

              } else if (type.equals(DiscoveryType.REMOVED)
                  && (this.serviceRegistry.unregisterService(serviceEndpoint.id()) != null)) {

                LOGGER.info(
                    "Service Reference was REMOVED since Member have left the cluster {} : {}",
                    member,
                    serviceEndpoint);

                DiscoveryEvent registrationEvent = DiscoveryEvent.unregistered(serviceEndpoint);
                LOGGER.debug("Publish unregistered: " + registrationEvent);
                sink.next(registrationEvent);
              }
            });
  }
}
