package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServiceRegistryImpl implements ServiceRegistry {

  // todo how to remove it (tags problem)?
  private final ConcurrentMap<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<ServiceReference>> byQualifier = new ConcurrentHashMap<>();

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    // todo reverse from byQualifier
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return serviceReferenceStream().collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> lookupService(String qualifier) {
    return byQualifier.getOrDefault(qualifier, Collections.emptyList());
  }

  @Override
  public List<ServiceReference> lookupService(Predicate<? super ServiceReference> filter) {
    // Convert to stream of service references
    Stream<ServiceReference> stream = serviceReferenceStream();

    // Filter by filter
    if (filter != null) {
      stream = stream.filter(filter);
    }

    // Collect results
    return stream.collect(Collectors.toList());
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    // todo simplify it
    boolean result = serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (result) {
      serviceEndpoint.serviceRegistrations().stream().flatMap(
          sr -> sr.methods().stream().map(
              sm -> new ServiceReference(sm, sr, serviceEndpoint)))
          .forEach(
              reference -> {
                byQualifier.computeIfAbsent(reference.qualifier(), k -> new CopyOnWriteArrayList<>())
                    .add(reference);
                // todo add to byTags
              });
    }
    return result;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    byQualifier.values().forEach(list -> list.removeIf(sr -> sr.endpointId().equals(endpointId)));
    // todo remove from byTags
    return serviceEndpoint;
  }

  private Stream<ServiceReference> serviceReferenceStream() {
    // return serviceEndpoints.values().stream().flatMap(
    // se -> se.serviceRegistrations().stream().flatMap(
    // sr -> sr.methods().stream().map(
    // sm -> new ServiceReference(sm, sr, se))));
    return byQualifier.values().stream().flatMap(Collection::stream).distinct();
  }
}
