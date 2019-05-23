package io.scalecube.services.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
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

  /**
   * Construcotr.
   *
   * @param serviceEndpoint service endpint
   */
  public ScalecubeServiceGroupDiscovery(ServiceEndpoint serviceEndpoint) {
    this(serviceEndpoint, ClusterConfig.defaultLanConfig());
  }

  /**
   * Construcotr.
   *
   * @param serviceEndpoint service endpoint
   * @param clusterConfig cluster config
   */
  public ScalecubeServiceGroupDiscovery(
      ServiceEndpoint serviceEndpoint, ClusterConfig clusterConfig) {
    this.serviceEndpoint = serviceEndpoint;
    this.clusterConfig = clusterConfig;
    // Add myself to the group if grouping tags are present
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();
    if (serviceGroup != null) {
      addToGroup(serviceGroup.id(), serviceEndpoint);
    }
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
      ServiceDiscoveryEvent discoveryEvent, SynchronousSink<ServiceGroupDiscoveryEvent> sink) {

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

    if (discoveryEvent.isRegistered()) {
      Collection<ServiceEndpoint> endpoints = addToGroup(groupId, serviceEndpoint);

      LOGGER.trace(
          "Added service endpoint {} to group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      if (endpoints.size() == serviceGroup.size()) {
        LOGGER.info("Service group {} added to the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceGroupDiscoveryEvent.registered(groupId, endpoints);
      }
    }
    if (discoveryEvent.isUnregistered() && endpointGroups.containsKey(groupId)) {
      Collection<ServiceEndpoint> endpoints = removeFromGroup(groupId, serviceEndpoint);

      LOGGER.trace(
          "Removed service endpoint {} from group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          Optional.ofNullable(endpoints).map(Collection::size).orElse(0));

      if (endpoints == null || containsOnlySelf(endpoints)) {
        LOGGER.info("Service group {} removed from the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceGroupDiscoveryEvent.unregistered(groupId);
      }
    }

    if (groupDiscoveryEvent != null) {
      sink.next(groupDiscoveryEvent);
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
