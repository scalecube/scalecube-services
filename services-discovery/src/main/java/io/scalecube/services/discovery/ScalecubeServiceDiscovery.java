package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private final ServiceEndpoint endpoint;
  private final ClusterConfig clusterConfig;

  private Cluster cluster;

  private final EmitterProcessor<ServiceDiscoveryEvent> subject = EmitterProcessor.create(65536);
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.serialize().sink();

  /**
   * Constructor.
   *
   * @param endpoint service endpoiintg
   * @param clusterConfig slcaluecibe cluster config
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint endpoint, ClusterConfig clusterConfig) {
    this.endpoint = endpoint;
    this.clusterConfig = clusterConfig;
  }

  /**
   * Constructror with default {@code ClusterConfig.defaultLanConfig}.
   *
   * @param endpoint service endpoiint
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint endpoint) {
    this(endpoint, ClusterConfig.defaultLanConfig());
  }

  private ScalecubeServiceDiscovery(ScalecubeServiceDiscovery that, ClusterConfig clusterConfig) {
    this(that.endpoint, clusterConfig);
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
  @Override
  public Mono<ServiceDiscovery> start() {
    return Mono.defer(
        () -> {
          Map<String, String> metadata =
              endpoint != null
                  ? Collections.singletonMap(
                      endpoint.id(), ClusterMetadataCodec.encodeMetadata(endpoint))
                  : Collections.emptyMap();

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
    return subject;
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> {
          sink.complete();
          return Optional.ofNullable(cluster).map(Cluster::shutdown).orElse(Mono.empty());
        });
  }

  private void onMemberEvent(MembershipEvent membershipEvent) {
    final Member member = membershipEvent.member();

    Map<String, String> metadata = null;
    if (membershipEvent.isAdded()) {
      metadata = membershipEvent.newMetadata();
      LOGGER.info("ServiceEndpoint added, since member {} has joined the cluster", member);
    }
    if (membershipEvent.isRemoved()) {
      metadata = membershipEvent.oldMetadata();
      LOGGER.info("ServiceEndpoint removed, since member {} have left the cluster", member);
    }

    List<ServiceEndpoint> serviceEndpoints =
        Optional.ofNullable(metadata).orElse(Collections.emptyMap()).values().stream()
            .map(ClusterMetadataCodec::decodeMetadata)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    serviceEndpoints.forEach(
        serviceEndpoint -> {
          ServiceDiscoveryEvent discoveryEvent = null;

          if (membershipEvent.isAdded()) {
            discoveryEvent = ServiceDiscoveryEvent.registered(serviceEndpoint);
          }
          if (membershipEvent.isRemoved()) {
            discoveryEvent = ServiceDiscoveryEvent.unregistered(serviceEndpoint);
          }

          if (discoveryEvent != null) {
            sink.next(discoveryEvent);
          }
        });
  }
}
