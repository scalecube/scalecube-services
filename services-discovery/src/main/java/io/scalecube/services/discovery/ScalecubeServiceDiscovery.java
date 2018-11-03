package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterConfig.Builder;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryConfig;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.Address;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  public static final String SERVICE_METADATA = "service";

  private ServiceRegistry serviceRegistry;
  private Cluster cluster;
  private ServiceEndpoint endpoint;

  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.serialize().sink();

  @Override
  public Address address() {
    io.scalecube.transport.Address address = cluster.address();
    return Address.create(address.host(), address.port());
  }

  @Override
  public ServiceEndpoint endpoint() {
    return this.endpoint;
  }

  @Override
  public Mono<ServiceDiscovery> start(ServiceDiscoveryConfig config) {
    return Mono.defer(
        () -> {
          this.serviceRegistry = config.serviceRegistry();
          this.endpoint = config.endpoint();

          ClusterConfig clusterConfig =
              clusterConfigBuilder(config).addMetadata(getMetadata()).build();

          LOGGER.info("Start scalecube service discovery with config: {}", clusterConfig);

          return Cluster.join(clusterConfig)
              .doOnSuccess(cluster -> this.cluster = cluster)
              .doOnSuccess(this::listen)
              .thenReturn(this);
        });
  }

  private Map<String, String> getMetadata() {
    return serviceRegistry
        .listServiceEndpoints()
        .stream()
        .collect(
            Collectors.toMap(ClusterMetadataDecoder::encodeMetadata, service -> SERVICE_METADATA));
  }

  private void listen(Cluster cluster) {
    cluster.listenMembership().subscribe(this::onMemberEvent);
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listen() {
    return Flux.defer(
        () ->
            Flux.fromIterable(serviceRegistry.listServiceEndpoints())
                .map(ServiceDiscoveryEvent::registered)
                .concatWith(subject));
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> {
          sink.complete();
          return Optional.ofNullable(cluster).map(Cluster::shutdown).orElse(Mono.empty());
        });
  }

  private ClusterConfig.Builder clusterConfigBuilder(ServiceDiscoveryConfig config) {
    Builder builder = ClusterConfig.builder();
    if (config.seeds() != null) {
      builder.seedMembers(
          Arrays.stream(config.seeds())
              .map(address -> io.scalecube.transport.Address.create(address.host(), address.port()))
              .toArray(io.scalecube.transport.Address[]::new));
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

  private void onMemberEvent(MembershipEvent event) {
    Member member = event.member();
    member
        .metadata()
        .entrySet()
        .stream()
        .filter(entry -> SERVICE_METADATA.equals(entry.getValue()))
        .map(Entry::getKey)
        .map(ClusterMetadataDecoder::decodeMetadata)
        .filter(Objects::nonNull)
        .forEach(
            serviceEndpoint -> {
              // Register services
              if (event.isAdded() && serviceRegistry.registerService(serviceEndpoint)) {
                LOGGER.info(
                    "ServiceEndpoint was ADDED since new Member has joined the cluster {} : {}",
                    member,
                    serviceEndpoint);

                ServiceDiscoveryEvent discoveryEvent =
                    ServiceDiscoveryEvent.registered(serviceEndpoint);
                LOGGER.info("Publish services registered: {}", discoveryEvent);

                sink.next(discoveryEvent);
              }
              // Unregister services
              if (event.isRemoved()
                  && serviceRegistry.unregisterService(serviceEndpoint.id()) != null) {
                LOGGER.info(
                    "ServiceEndpoint was REMOVED since Member have left the cluster {} : {}",
                    member,
                    serviceEndpoint);

                ServiceDiscoveryEvent discoveryEvent =
                    ServiceDiscoveryEvent.unregistered(serviceEndpoint);
                LOGGER.info("Publish services unregistered: {}", discoveryEvent);

                sink.next(discoveryEvent);
              }
            });
  }
}
