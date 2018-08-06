package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.RegistryEvent;
import io.scalecube.services.registry.api.RegistryEvent.Type;
import io.scalecube.services.registry.api.ServiceRegistry;

import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> referencesByQualifier = new NonBlockingHashMap<>();

  private final FluxProcessor<RegistryEvent, RegistryEvent> events =
      DirectProcessor.<RegistryEvent>create().serialize();

  private final FluxSink<RegistryEvent> sink = events.sink();

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
  public List<ServiceReference> lookupService(String qualifier) {
    List<ServiceReference> result = referencesByQualifier.get(qualifier);
    return result != null ? Collections.unmodifiableList(result) : Collections.emptyList();
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
            list.stream().filter(sr -> sr.endpointId().equals(endpointId)).collect(Collectors.toSet())
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

  public Flux<RegistryEvent> listen() {
    return Flux.fromIterable(referencesByQualifier.values()).flatMap(Flux::fromIterable)
        .map(sr -> new RegistryEvent(Type.ADDED, sr))
        .concatWith(events);
  }
}
