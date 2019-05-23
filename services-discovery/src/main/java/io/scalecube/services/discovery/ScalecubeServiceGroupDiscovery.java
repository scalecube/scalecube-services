package io.scalecube.services.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceGroupDiscovery;
import io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class ScalecubeServiceGroupDiscovery implements ServiceGroupDiscovery {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ScalecubeServiceGroupDiscovery.class);

  private final ServiceEndpoint serviceEndpoint;
  private final ClusterConfig clusterConfig;

  private ServiceDiscovery serviceDiscovery;
  private Map<String, Collection<ServiceEndpoint>> endpointGroups = new HashMap<>();

  public ScalecubeServiceGroupDiscovery(ServiceEndpoint serviceEndpoint) {
    this(serviceEndpoint, ClusterConfig.defaultLanConfig());
  }

  public ScalecubeServiceGroupDiscovery(
      ServiceEndpoint serviceEndpoint, ClusterConfig clusterConfig) {

    Map<String, String> tags = serviceEndpoint.tags();
    String groupId = tags.get(ServiceGroup.GROUP_ID);
    String groupSize = tags.get(ServiceGroup.GROUP_SIZE);

    // Add myself to the group if grouping tags are present
    if (groupId != null && groupSize != null) {
      addToGroup(groupId, serviceEndpoint);
    }

    this.serviceEndpoint = serviceEndpoint;
    this.clusterConfig = clusterConfig;
  }

  private ScalecubeServiceGroupDiscovery(
      ScalecubeServiceGroupDiscovery other, ServiceDiscovery serviceDiscovery) {
    this(other.serviceEndpoint, other.clusterConfig);
    this.serviceDiscovery = serviceDiscovery;
  }

  @Override
  public Mono<ServiceGroupDiscovery> start() {
    return Mono.defer(() -> new ScalecubeServiceDiscovery(serviceEndpoint, clusterConfig).start())
        .flatMap(discovery -> Mono.just(new ScalecubeServiceGroupDiscovery(this, discovery)));
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () ->
            Optional.ofNullable(serviceDiscovery)
                .map(ServiceDiscovery::shutdown)
                .orElse(Mono.empty()));
  }

  @Override
  public Flux<ServiceGroupDiscoveryEvent> listen() {
    return serviceDiscovery.listen().handle(this::onDiscoveryEvent);
  }

  private void onDiscoveryEvent(
      ServiceDiscoveryEvent event, SynchronousSink<ServiceGroupDiscoveryEvent> sink) {

    ServiceEndpoint serviceEndpoint = event.serviceEndpoint();
    Map<String, String> tags = serviceEndpoint.tags();
    String groupId = tags.get(ServiceGroup.GROUP_ID);
    Optional<String> groupSizeOptional = Optional.ofNullable(tags.get(ServiceGroup.GROUP_SIZE));

    if (groupId == null) {
      LOGGER.trace("Discovered (but not registering) {}", serviceEndpoint);
      return;
    }
    if (!groupSizeOptional.isPresent()) {
      LOGGER.error("Group size is absent in discovered member {}", serviceEndpoint);
      return;
    }

    int groupSize = groupSizeOptional.map(Integer::valueOf).orElse(null);
    ServiceGroupDiscoveryEvent discoveryEvent = null;

    if (event.isRegistered()) {
      Collection<ServiceEndpoint> endpoints = addToGroup(groupId, serviceEndpoint);

      LOGGER.trace("Added to group {} (size now {})", groupId, endpoints.size());

      if (endpoints.size() == groupSize) {
        ServiceGroup group = new ServiceGroup(groupId, endpoints);
        LOGGER.info("Service group added to the cluster: {}", group);
        discoveryEvent = ServiceGroupDiscoveryEvent.registered(group);
      }
    }
    if (event.isUnregistered() && endpointGroups.containsKey(groupId)) {
      Collection<ServiceEndpoint> endpoints = removeFromGroup(groupId, serviceEndpoint);

      LOGGER.trace(
          "Removed from group {} (size now {})",
          groupId,
          Optional.ofNullable(endpoints).map(Collection::size).orElse(0));

      if (endpoints == null || containsOnlySelf(endpoints)) {
        ServiceGroup group = new ServiceGroup(groupId);
        LOGGER.info("Service group removed from the cluster: {}", group);
        discoveryEvent = ServiceGroupDiscoveryEvent.unregistered(group);
      }
    }

    if (discoveryEvent != null) {
      sink.next(discoveryEvent);
    }
  }

  private boolean containsOnlySelf(Collection<ServiceEndpoint> endpoints) {
    return endpoints.size() == 1 && endpoints.contains(serviceEndpoint);
  }

  private Collection<ServiceEndpoint> addToGroup(String groupId, ServiceEndpoint endpoint) {
    return endpointGroups.compute(
        groupId,
        (key, endpoints) -> {
          if (endpoints == null) {
            endpoints = new ArrayList<>();
          }
          endpoints.add(endpoint);
          return endpoints;
        });
  }

  private Collection<ServiceEndpoint> removeFromGroup(String groupId, ServiceEndpoint endpoint) {
    return endpointGroups.compute(
        groupId,
        (key, endpoints) -> {
          if (endpoints == null) {
            return null;
          }
          endpoints.removeIf(input -> input.id().equals(endpoint.id()));
          return endpoints.isEmpty() ? null : endpoints;
        });
  }
}
