package io.scalecube.services.discovery;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public final class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER =
      LoggerFactory.getLogger("io.scalecube.services.discovery.ServiceDiscovery");
  private static final Logger LOGGER_GROUP =
      LoggerFactory.getLogger("io.scalecube.services.discovery.ServiceGroupDiscovery");

  private final ServiceEndpoint serviceEndpoint;
  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.sink();
  private ClusterConfig clusterConfig =
      ClusterConfig.from(ClusterConfig.defaultLanConfig()).build();
  private Cluster cluster;
  private Map<ServiceGroup, Collection<ServiceEndpoint>> groups = new HashMap<>();

  /**
   * Constructor.
   *
   * @param serviceEndpoint service endpoint
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;

    // Add myself to the group if 'groupness' is defined
    Optional.ofNullable(serviceEndpoint.serviceGroup())
        .ifPresent(serviceGroup -> addToGroup(serviceGroup, serviceEndpoint));

    // Add local metadata
    Map<String, String> metadata =
        Collections.singletonMap(
            serviceEndpoint.id(), ClusterMetadataCodec.encodeMetadata(serviceEndpoint));
    clusterConfig = ClusterConfig.from(clusterConfig).addMetadata(metadata).build();
  }

  /**
   * Copy constructor.
   *
   * @param other other instance
   */
  private ScalecubeServiceDiscovery(ScalecubeServiceDiscovery other) {
    this.serviceEndpoint = other.serviceEndpoint;
    this.clusterConfig = other.clusterConfig;
    this.cluster = other.cluster;
    this.groups = other.groups;
  }

  /**
   * Setter for {@code ClusterConfig.Builder} options.
   *
   * @param opts ClusterConfig options builder
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig.Builder> opts) {
    ScalecubeServiceDiscovery d = new ScalecubeServiceDiscovery(this);
    d.clusterConfig = opts.apply(ClusterConfig.from(clusterConfig)).build();
    return d;
  }

  @Override
  public Address address() {
    return cluster.address();
  }

  @Override
  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  /**
   * Starts scalecube service discovery. Joins a cluster with local services as metadata.
   *
   * @return mono result
   */
  @Override
  public Mono<ServiceDiscovery> start() {
    return Mono.defer(
        () -> {
          // Start scalecube-cluster and listen membership events
          return new ClusterImpl()
              .config(options -> ClusterConfig.from(clusterConfig))
              .handler(
                  cluster -> {
                    return new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        ScalecubeServiceDiscovery.this.onMembershipEvent(event, sink);
                      }
                    };
                  })
              .start()
              .doOnSuccess(
                  cluster -> {
                    this.cluster = cluster;
                    LOGGER.debug("Started {} with config -- {}", cluster, clusterConfig);
                  })
              .thenReturn(this);
        });
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return subject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () ->
            Optional.ofNullable(cluster) //
                .map(Cluster::shutdown)
                .orElse(Mono.empty())
                .then(Mono.fromRunnable(sink::complete)));
  }

  private void onMembershipEvent(
      MembershipEvent membershipEvent, FluxSink<ServiceDiscoveryEvent> sink) {

    if (membershipEvent.isAdded()) {
      LOGGER.debug("Member {} has joined the cluster", membershipEvent.member());
    }
    if (membershipEvent.isRemoved()) {
      LOGGER.debug("Member {} has left the cluster", membershipEvent.member());
    }

    ServiceEndpoint serviceEndpoint = getServiceEndpoint(membershipEvent);

    if (serviceEndpoint == null) {
      return;
    }

    if (membershipEvent.isAdded()) {
      LOGGER.info(
          "Service endpoint {} is about to be added, since member {} has joined the cluster",
          serviceEndpoint.id(),
          membershipEvent.member());
    }
    if (membershipEvent.isRemoved()) {
      LOGGER.info(
          "Service endpoint {} is about to be removed, since member {} have left the cluster",
          serviceEndpoint.id(),
          membershipEvent.member());
    }

    ServiceDiscoveryEvent discoveryEvent = null;

    if (membershipEvent.isAdded()) {
      discoveryEvent = ServiceDiscoveryEvent.newEndpointAdded(serviceEndpoint);
    }
    if (membershipEvent.isRemoved()) {
      discoveryEvent = ServiceDiscoveryEvent.newEndpointRemoved(serviceEndpoint);
    }

    if (discoveryEvent != null) {
      sink.next(discoveryEvent);
      onDiscoveryEvent(discoveryEvent, sink);
    }
  }

  private void onDiscoveryEvent(
      ServiceDiscoveryEvent discoveryEvent, FluxSink<ServiceDiscoveryEvent> sink) {

    ServiceEndpoint serviceEndpoint = discoveryEvent.serviceEndpoint();
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();
    if (serviceGroup == null) {
      LOGGER_GROUP.debug(
          "Discovered service endpoint {}, but not registering it (serviceGroup is null)",
          serviceEndpoint.id());
      return;
    }

    ServiceDiscoveryEvent groupDiscoveryEvent = null;
    String groupId = serviceGroup.id();

    if (discoveryEvent.isEndpointAdded()) {
      if (!addToGroup(serviceGroup, serviceEndpoint)) {
        LOGGER_GROUP.warn(
            "Failed to add service endpoint {} to group {}, group is full aready",
            serviceEndpoint.id(),
            groupId);
        return;
      }

      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      sink.next(ServiceDiscoveryEvent.newEndpointAddedToGroup(groupId, serviceEndpoint, endpoints));

      LOGGER_GROUP.debug(
          "Added service endpoint {} to group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      if (endpoints.size() == serviceGroup.size()) {
        LOGGER_GROUP.info("Service group {} added to the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceDiscoveryEvent.newGroupAdded(groupId, endpoints);
      }
    }

    if (discoveryEvent.isEndpointRemoved()) {
      if (!removeFromGroup(serviceGroup, serviceEndpoint)) {
        LOGGER_GROUP.warn(
            "Failed to remove service endpoint {} from group {}, "
                + "there were no such group or service endpoint was never registered in group",
            serviceEndpoint.id(),
            groupId);
        return;
      }

      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      sink.next(
          ServiceDiscoveryEvent.newEndpointRemovedFromGroup(groupId, serviceEndpoint, endpoints));

      LOGGER_GROUP.debug(
          "Removed service endpoint {} from group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      if (endpoints.isEmpty()) {
        LOGGER_GROUP.info("Service group {} removed from the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceDiscoveryEvent.newGroupRemoved(groupId);
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

  private ServiceEndpoint getServiceEndpoint(MembershipEvent membershipEvent) {
    Map<String, String> metadata = null;

    if (membershipEvent.isAdded()) {
      metadata = membershipEvent.newMetadata();
    }
    if (membershipEvent.isRemoved()) {
      metadata = membershipEvent.oldMetadata();
    }

    if (metadata == null) {
      return null;
    }

    String metadataValue = metadata.values().stream().findFirst().orElse(null);
    return ClusterMetadataCodec.decodeMetadata(metadataValue);
  }
}
