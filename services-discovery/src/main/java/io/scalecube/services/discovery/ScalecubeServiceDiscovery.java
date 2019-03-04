package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.Address;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private final ServiceRegistry serviceRegistry;
  private final ServiceEndpoint endpoint;
  private final ClusterConfig clusterConfig;

  private Cluster cluster;

  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.serialize().sink();

  /**
   * Constructor.
   *
   * @param serviceRegistry service registrety
   * @param endpoint service endpoiintg
   * @param clusterConfig slcaluecibe cluster config
   */
  public ScalecubeServiceDiscovery(
      ServiceRegistry serviceRegistry, ServiceEndpoint endpoint, ClusterConfig clusterConfig) {
    this.serviceRegistry = serviceRegistry;
    this.endpoint = endpoint;
    this.clusterConfig = clusterConfig;
  }

  /**
   * Constructror.
   *
   * @param serviceRegistry service registry
   * @param endpoint service endpoiint
   */
  public ScalecubeServiceDiscovery(ServiceRegistry serviceRegistry, ServiceEndpoint endpoint) {
    this(serviceRegistry, endpoint, ClusterConfig.defaultLanConfig());
  }

  private ScalecubeServiceDiscovery(ScalecubeServiceDiscovery that, ClusterConfig clusterConfig) {
    this(that.serviceRegistry, that.endpoint, clusterConfig);
  }

  private ClusterConfig.Builder copyFrom(ClusterConfig config) {
    return ClusterConfig.builder()
        .seedMembers(config.getSeedMembers())
        .metadataTimeout(config.getMetadataTimeout())
        .metadata(config.getMetadata())
        .memberHost(config.getMemberHost())
        .memberPort(config.getMemberPort())
        .gossipFanout(config.getGossipFanout())
        .gossipInterval(config.getGossipInterval())
        .gossipRepeatMult(config.getGossipRepeatMult())
        .pingInterval(config.getPingInterval())
        .pingReqMembers(config.getPingReqMembers())
        .pingTimeout(config.getPingTimeout())
        .suspicionMult(config.getSuspicionMult())
        .syncGroup(config.getSyncGroup())
        .syncInterval(config.getSyncInterval())
        .syncTimeout(config.getSyncTimeout())
        .transportConfig(config.getTransportConfig());
  }

  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig.Builder> opts) {
    return new ScalecubeServiceDiscovery(this, opts.apply(copyFrom(clusterConfig)).build());
  }

  @Override
  public Address address() {
    return Address.create(cluster.address().host(), cluster.address().port());
  }

  @Override
  public ServiceEndpoint endpoint() {
    return endpoint;
  }

  /**
   * Starts scalecube service discoevery. Joins a cluster with local services as metadata.
   *
   * @return mono result
   */
  public Mono<ServiceDiscovery> start() {
    return Mono.defer(
        () -> {
          Map<String, String> metadata =
              serviceRegistry.listServiceEndpoints().stream()
                  .collect(
                      Collectors.toMap(
                          ServiceEndpoint::id, ClusterMetadataDecoder::encodeMetadata));

          ClusterConfig clusterConfig = copyFrom(this.clusterConfig).addMetadata(metadata).build();
          ScalecubeServiceDiscovery serviceDiscovery =
              new ScalecubeServiceDiscovery(this, clusterConfig);

          LOGGER.info("Start ScalecubeServiceDiscovery with config: {}", clusterConfig);
          return Cluster.join(clusterConfig)
              .doOnSuccess(cluster -> serviceDiscovery.cluster = cluster)
              .doOnSuccess(serviceDiscovery::listen)
              .thenReturn(serviceDiscovery);
        });
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

  private void onMemberEvent(MembershipEvent event) {
    final Member member = event.member();

    Map<String, String> metadata = null;
    if (event.isAdded()) {
      metadata = event.newMetadata();
      LOGGER.info("ServiceEndpoint ADDED, since member {} has joined the cluster", member);
    }
    if (event.isRemoved()) {
      metadata = event.oldMetadata();
      LOGGER.info("ServiceEndpoint REMOVED, since member {} have left the cluster", member);
    }

    List<ServiceEndpoint> serviceEndpoints =
        Optional.ofNullable(metadata).orElse(Collections.emptyMap()).values().stream()
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
                discoveryEvent = ServiceDiscoveryEvent.registered(serviceEndpoint);
              }
              break;
            case REMOVED:
              // Unregister services
              if (serviceRegistry.unregisterService(serviceEndpoint.id()) != null) {
                discoveryEvent = ServiceDiscoveryEvent.unregistered(serviceEndpoint);
              }
              break;
            default:
              break;
          }

          if (discoveryEvent != null) {
            switch (discoveryEvent.type()) {
              case REGISTERED:
                LOGGER.info("Publish services registered: {}", discoveryEvent);
                break;
              case UNREGISTERED:
                LOGGER.info("Publish services unregistered: {}", discoveryEvent);
                break;
              default:
                break;
            }
            sink.next(discoveryEvent);
          }
        });
  }
}
