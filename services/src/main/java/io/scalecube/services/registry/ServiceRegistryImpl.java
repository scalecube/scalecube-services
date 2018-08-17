package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.RegistryEvent;
import io.scalecube.services.registry.api.RegistryEvent.Type;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jctools.maps.NonBlockingHashMap;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;

public class ServiceRegistryImpl implements ServiceRegistry {

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> referencesByQualifier =
      new NonBlockingHashMap<>();

  private final FluxProcessor<RegistryEvent, RegistryEvent> events =
      DirectProcessor.<RegistryEvent>create();

  private final FluxSink<RegistryEvent> sink = events.serialize().sink();

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    // todo how to collect tags correctly?
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return serviceReferenceStream().collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> lookupService(ServiceMessage request) {
    List<ServiceReference> result = referencesByQualifier.get(request.qualifier());
    if (result == null || result.isEmpty()) {
      return Collections.emptyList();
    }
    String contentType = request.dataFormatOrDefault();
    return result
        .stream()
        .filter(ref -> ref.contentTypes().contains(contentType))
        .collect(Collectors.toList());
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    boolean success = serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (success) {
      serviceEndpoint
          .serviceRegistrations()
          .stream()
          .flatMap(serviceRegistration -> serviceRegistration.methods().stream()
              .map(sm -> new ServiceReference(sm, serviceRegistration, serviceEndpoint)))
          .forEach(serviceReference -> {
            referencesByQualifier
                .computeIfAbsent(serviceReference.qualifier(), key -> new CopyOnWriteArrayList<>())
                .add(serviceReference);
            sink.next(new RegistryEvent(Type.ADDED, serviceReference));
          });
    }
    return success;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      referencesByQualifier.values()
          .forEach(list -> {
            list.stream().filter(sr -> sr.endpointId().equals(endpointId))
                .forEach(sr -> {
                  list.remove(sr);
                  sink.next(new RegistryEvent(Type.REMOVED, sr));
                });
          });
    }
    
    return serviceEndpoint;
  }

  Stream<ServiceReference> serviceReferenceStream() {
    return referencesByQualifier.values().stream().flatMap(Collection::stream);
  }

  /**
   * listen on service registry events.
   */
  public Flux<RegistryEvent> listen() {
    return Flux.fromIterable(referencesByQualifier.values()).flatMap(Flux::fromIterable)
        .map(sr -> new RegistryEvent(Type.ADDED, sr))
        .concatWith(events);
  }
}
