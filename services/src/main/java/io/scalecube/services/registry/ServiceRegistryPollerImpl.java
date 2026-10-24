package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistryPoller;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

final class ServiceRegistryPollerImpl implements ServiceRegistryPoller {

  private final AtomicLong version;
  private final Map<String, ServiceEndpoint> serviceEndpoints;

  private long seenVersion = -1;
  private Map<String, ServiceEndpoint> endpoints = new HashMap<>();
  private Collection<ServiceEndpoint> endpointsView =
      Collections.unmodifiableCollection(endpoints.values());

  ServiceRegistryPollerImpl(AtomicLong version, Map<String, ServiceEndpoint> serviceEndpoints) {
    this.version = version;
    this.serviceEndpoints = serviceEndpoints;
  }

  @Override
  public int poll(Consumer<ServiceEndpoint> onAdded, Consumer<ServiceEndpoint> onRemoved) {
    final var currentVersion = version.get(); // before the endpoints
    if (currentVersion == seenVersion) {
      return 0;
    }
    seenVersion = currentVersion;

    final var previous = endpoints;
    final var current = new HashMap<>(serviceEndpoints);
    endpoints = current;
    endpointsView = Collections.unmodifiableCollection(current.values());

    var changes = 0;
    for (var endpoint : previous.values()) {
      if (!current.containsKey(endpoint.id())) {
        onRemoved.accept(endpoint);
        changes++;
      }
    }
    for (var endpoint : current.values()) {
      if (!previous.containsKey(endpoint.id())) {
        onAdded.accept(endpoint);
        changes++;
      }
    }
    return changes;
  }

  @Override
  public Collection<ServiceEndpoint> endpoints() {
    return endpointsView;
  }
}
