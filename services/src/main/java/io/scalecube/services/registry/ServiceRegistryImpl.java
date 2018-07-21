package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.RegistrationEvent;
import io.scalecube.services.registry.api.RegistrationEvent.Type;
import io.scalecube.services.registry.api.ServiceRegistry;

import org.jctools.maps.NonBlockingHashMap;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Schedulers;

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

  private final DirectProcessor<RegistrationEvent> subject = DirectProcessor.create();

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> referencesByQualifier = new NonBlockingHashMap<>();


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
      serviceEndpoint.serviceRegistrations().stream().flatMap(
          sr -> sr.methods().stream().map(
              sm -> new ServiceReference(sm, sr, serviceEndpoint)))
          .forEach(
              reference -> referencesByQualifier
                  .computeIfAbsent(reference.qualifier(), k -> new CopyOnWriteArrayList<>())
                  .add(reference));

      subject.onNext(new RegistrationEvent(Type.ADDED, serviceEndpoint));
    }
    return success;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      referencesByQualifier.values().forEach(list -> list.removeIf(sr -> sr.endpointId().equals(endpointId)));
      subject.onNext(new RegistrationEvent(Type.REMOVED, serviceEndpoint));
    }
    return serviceEndpoint;
  }

  @Override
  public Flux<RegistrationEvent> listen() {
    return Flux.fromStream(serviceEndpoints.values().stream())
        .map(serviceEndpoint->new RegistrationEvent(Type.ADDED, serviceEndpoint))
        .concatWith(subject)
        .publishOn(Schedulers.single());
  }

  Stream<ServiceReference> serviceReferenceStream() {
    return referencesByQualifier.values().stream().flatMap(Collection::stream);
  }
}
