package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScalecubeServiceDiscovery.class);

  private final ServiceEndpoint serviceEndpoint;
  private final ClusterConfig clusterConfig;

  private Cluster cluster;

  private Map<ServiceGroup, Collection<ServiceEndpoint>> groups = new HashMap<>();

  /**
   * Constructor.
   *
   * @param serviceEndpoint service endpoiintg
   * @param clusterConfig slcaluecibe cluster config
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint serviceEndpoint, ClusterConfig clusterConfig) {
    this.serviceEndpoint = serviceEndpoint;
    this.clusterConfig = clusterConfig;
    // Add myself to the group if 'groupness' is defined
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();
    if (serviceGroup != null) {
      addToGroup(serviceGroup, serviceEndpoint);
    }
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
    this(that.serviceEndpoint, clusterConfig);
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
  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
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
              serviceEndpoint != null
                  ? Collections.singletonMap(
                      serviceEndpoint.id(), ClusterMetadataCodec.encodeMetadata(serviceEndpoint))
                  : Collections.emptyMap();

          ClusterConfig newClusterConfig = //
              copyFrom(clusterConfig).addMetadata(metadata).build();

          ScalecubeServiceDiscovery serviceDiscovery =
              new ScalecubeServiceDiscovery(this, newClusterConfig);

          return Cluster.join(newClusterConfig)
              .doOnSuccess(cluster -> serviceDiscovery.cluster = cluster)
              .thenReturn(serviceDiscovery);
        });
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return cluster
        .listenMembership()
        .flatMap(event -> Flux.create(sink -> onMembershipEvent(event, sink)));
  }

  @Override
  public Flux<ServiceGroupDiscoveryEvent> listenGroupDiscovery() {
    return listenDiscovery().flatMap(event -> Flux.create(sink -> onDiscoveryEvent(event, sink)));
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> Optional.ofNullable(cluster).map(Cluster::shutdown).orElse(Mono.empty()));
  }

  private void onMembershipEvent(
      MembershipEvent membershipEvent, FluxSink<ServiceDiscoveryEvent> sink) {
    final Member member = membershipEvent.member();

    Map<String, String> metadata = null;
    if (membershipEvent.isAdded()) {
      metadata = membershipEvent.newMetadata();
      LOGGER.info("Service endpoint added, since member {} has joined the cluster", member);
    }
    if (membershipEvent.isRemoved()) {
      metadata = membershipEvent.oldMetadata();
      LOGGER.info("Service endpoint removed, since member {} have left the cluster", member);
    }

    Optional.ofNullable(metadata).orElse(Collections.emptyMap()).values().stream()
        .map(ClusterMetadataCodec::decodeMetadata)
        .filter(Objects::nonNull)
        .forEach(
            serviceEndpoint -> {
              ServiceDiscoveryEvent discoveryEvent = null;

              if (membershipEvent.isAdded()) {
                discoveryEvent = ServiceDiscoveryEvent.newEndpointAdded(serviceEndpoint);
              }
              if (membershipEvent.isRemoved()) {
                discoveryEvent = ServiceDiscoveryEvent.newEndpointRemoved(serviceEndpoint);
              }

              if (discoveryEvent != null) {
                sink.next(discoveryEvent);
              }
            });
  }

  private void onDiscoveryEvent(
      ServiceDiscoveryEvent discoveryEvent, FluxSink<ServiceGroupDiscoveryEvent> sink) {

    ServiceEndpoint serviceEndpoint = discoveryEvent.serviceEndpoint();
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();
    if (serviceGroup == null) {
      LOGGER.trace(
          "Discovered service endpoint {}, but not registering it (serviceGroup is null)",
          serviceEndpoint.id());
      return;
    }

    ServiceGroupDiscoveryEvent groupDiscoveryEvent = null;
    String groupId = serviceGroup.id();

    if (discoveryEvent.isEndpointAdded()) {
      if (!addToGroup(serviceGroup, serviceEndpoint)) {
        LOGGER.warn(
            "Failed to add service endpoint {} to group {}, group is full aready",
            serviceEndpoint.id(),
            groupId);
        return;
      }

      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      sink.next(
          ServiceGroupDiscoveryEvent.newEndpointAddedToGroup(groupId, serviceEndpoint, endpoints));

      LOGGER.trace(
          "Added service endpoint {} to group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      if (endpoints.size() == serviceGroup.size()) {
        LOGGER.info("Service group {} added to the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceGroupDiscoveryEvent.newGroupAdded(groupId, endpoints);
      }
    }
    if (discoveryEvent.isEndpointRemoved()) {
      if (!removeFromGroup(serviceGroup, serviceEndpoint)) {
        LOGGER.warn(
            "Failed to remove service endpoint {} from group {}, "
                + "there were no such group or service endpoint was never registered in group",
            serviceEndpoint.id(),
            groupId);
        return;
      }

      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      sink.next(
          ServiceGroupDiscoveryEvent.newEndpointRemovedFromGroup(
              groupId, serviceEndpoint, endpoints));

      LOGGER.trace(
          "Removed service endpoint {} from group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      if (endpoints.isEmpty()) {
        LOGGER.info("Service group {} removed from the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceGroupDiscoveryEvent.newGroupRemoved(groupId);
      }
    }

    if (groupDiscoveryEvent != null) {
      sink.next(groupDiscoveryEvent);
    }
  }

  public Collection<ServiceEndpoint> getEndpointsFromGroup(ServiceGroup group) {
    return groups.getOrDefault(group, Collections.emptyList());
  }

  private boolean addToGroup(ServiceGroup group, ServiceEndpoint endpoint) {
    Collection<ServiceEndpoint> endpoints =
        groups.computeIfAbsent(group, group1 -> new ArrayList<>());
    // check an actual group size is it still ok to add
    return endpoints.size() < group.size() && endpoints.add(endpoint);
  }

  private boolean removeFromGroup(ServiceGroup group, ServiceEndpoint endpoint) {
    if (!groups.containsKey(group)) {
      return false;
    }
    Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(group);
    boolean removed = endpoints.removeIf(input -> input.id().equals(endpoint.id()));
    if (removed && endpoints.isEmpty()) {
      groups.remove(group); // cleanup
    }
    return removed;
  }
}
