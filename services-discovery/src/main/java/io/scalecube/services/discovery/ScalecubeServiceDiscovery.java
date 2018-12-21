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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  private ServiceRegistry serviceRegistry;
  private Cluster cluster;
  private ServiceEndpoint endpoint;

  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.serialize().sink();

  @Override
  public Address address() {
    return toServicesAddress(cluster.address());
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
        .collect(Collectors.toMap(ServiceEndpoint::id, ClusterMetadataDecoder::encodeMetadata));
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

    Optional.ofNullable(config.seeds())
        .map(Arrays::stream)
        .map(stream -> stream.map(this::toClusterAddress))
        .map(stream -> stream.toArray(io.scalecube.transport.Address[]::new))
        .ifPresent(builder::seedMembers);

    Optional.ofNullable(config.port()).ifPresent(builder::port);
    Optional.ofNullable(config.tags()).ifPresent(builder::metadata);
    Optional.ofNullable(config.memberHost()).ifPresent(builder::memberHost);
    Optional.ofNullable(config.memberPort()).ifPresent(builder::memberPort);

    return builder;
  }

  private io.scalecube.transport.Address toClusterAddress(Address address) {
    return io.scalecube.transport.Address.create(address.host(), address.port());
  }

  private Address toServicesAddress(io.scalecube.transport.Address address) {
    return Address.create(address.host(), address.port());
  }

  private void onMemberEvent(MembershipEvent event) {
    final Member member = event.member();

    Map<String, String> metadata = null;
    if (event.isAdded()) {
      metadata = event.newMetadata();
    }
    if (event.isRemoved()) {
      metadata = event.oldMetadata();
    }

    List<ServiceEndpoint> serviceEndpoints =
        Optional.ofNullable(metadata)
            .orElse(Collections.emptyMap())
            .values()
            .stream()
            .map(ClusterMetadataDecoder::decodeMetadata)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    serviceEndpoints.forEach(
        serviceEndpoint -> {
          ServiceDiscoveryEvent discoveryEvent = null;

          switch (event.type()) {
            case ADDED:
              // Register services
              if (serviceRegistry.registerService(serviceEndpoint)) {
                LOGGER.info(
                    "ServiceEndpoint ADDED, since member {} has joined the cluster: {}",
                    member,
                    serviceEndpoint);
                discoveryEvent = ServiceDiscoveryEvent.registered(serviceEndpoint);
              }
              break;
            case REMOVED:
              // Unregister services
              if (serviceRegistry.unregisterService(serviceEndpoint.id()) != null) {
                LOGGER.info(
                    "ServiceEndpoint REMOVED, since member {} have left the cluster: {}",
                    member,
                    serviceEndpoint);
                discoveryEvent = ServiceDiscoveryEvent.unregistered(serviceEndpoint);
              }
              break;
            default:
              break;
          }

          if (discoveryEvent != null) {
            switch (discoveryEvent.type()) {
              case REGISTERED:
                LOGGER.info("Publish services unregistered: {}", discoveryEvent);
                break;
              case UNREGISTERED:
                LOGGER.info("Publish services registered: {}", discoveryEvent);
                break;
              default:
                break;
            }
            sink.next(discoveryEvent);
          }
        });
  }
}
