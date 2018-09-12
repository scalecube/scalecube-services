package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.EndpointRegistryEvent;
import io.scalecube.services.registry.api.ReferenceRegistryEvent;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jctools.maps.NonBlockingHashMap;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ServiceRegistryImpl implements ServiceRegistry {

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> referencesByQualifier =
      new NonBlockingHashMap<>();

  private final FluxProcessor<ReferenceRegistryEvent, ReferenceRegistryEvent> referenceEvents =
      DirectProcessor.create();
  private final FluxSink<ReferenceRegistryEvent> referenceEventSink =
      referenceEvents.serialize().sink();

  private final FluxProcessor<EndpointRegistryEvent, EndpointRegistryEvent> endpointEvents =
      DirectProcessor.create();
  private final FluxSink<EndpointRegistryEvent> endpointEventSink =
      endpointEvents.serialize().sink();

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
      List<ServiceReference> serviceReferences =
          serviceEndpoint
              .serviceRegistrations()
              .stream()
              .flatMap(
                  sr ->
                      sr.methods()
                          .stream()
                          .map(sm -> new ServiceReference(sm, sr, serviceEndpoint)))
              .collect(Collectors.toList());

      serviceReferences.forEach(
          sr ->
              referencesByQualifier
                  .computeIfAbsent(sr.qualifier(), key -> new CopyOnWriteArrayList<>())
                  .add(sr));

      serviceReferences.forEach(
          sr -> referenceEventSink.next(ReferenceRegistryEvent.createAdded(sr)));
      endpointEventSink.next(EndpointRegistryEvent.createAdded(serviceEndpoint));
    }
    return success;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      Map<String, ServiceReference> serviceReferencesOfEndpoint =
          referencesByQualifier
              .values()
              .stream()
              .flatMap(Collection::stream)
              .filter(sr -> sr.endpointId().equals(endpointId))
              .collect(Collectors.toMap(ServiceReference::qualifier, Function.identity()));

      serviceReferencesOfEndpoint.forEach(
          (qualifier, sr) -> {
            // do remapping
            referencesByQualifier.compute(
                qualifier,
                (qualifier1, list) -> {
                  list.remove(sr);
                  return !list.isEmpty() ? list : null;
                });
          });

      serviceReferencesOfEndpoint
          .values()
          .forEach(sr -> referenceEventSink.next(ReferenceRegistryEvent.createRemoved(sr)));
      endpointEventSink.next(EndpointRegistryEvent.createRemoved(serviceEndpoint));
    }

    return serviceEndpoint;
  }

  Stream<ServiceReference> serviceReferenceStream() {
    return referencesByQualifier.values().stream().flatMap(Collection::stream);
  }

  /**
   * Listen on service reference registry events.
   *
   * @return flux object
   */
  @Override
  public Flux<ReferenceRegistryEvent> listenReferenceEvents() {
    return Flux.fromIterable(referencesByQualifier.values())
        .flatMap(Flux::fromIterable)
        .map(ReferenceRegistryEvent::createAdded)
        .concatWith(referenceEvents);
  }

  /**
   * Listen on service endpoint registry events.
   *
   * @return flux object
   */
  @Override
  public Flux<EndpointRegistryEvent> listenEndpointEvents() {
    return Flux.fromIterable(serviceEndpoints.values())
        .map(EndpointRegistryEvent::createAdded)
        .concatWith(endpointEvents);
  }

  @Override
  public Mono<Void> close() {
    return Mono.create(
        sink -> {
          referenceEventSink.complete();
          endpointEventSink.complete();
          sink.success();
        });
  }
}
