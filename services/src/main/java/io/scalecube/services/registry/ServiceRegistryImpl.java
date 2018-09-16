package io.scalecube.services.registry;

import static io.scalecube.services.registry.api.RegistryEvent.Type.ADDED;
import static io.scalecube.services.registry.api.RegistryEvent.Type.REMOVED;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.RegistryEvent;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
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

  private final FluxProcessor<RegistryEvent, RegistryEvent> registryEvents =
      DirectProcessor.create();
  private final FluxSink<RegistryEvent> registryEventSink = registryEvents.serialize().sink();

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    // todo how to collect tags correctly?
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return referencesByQualifier
        .values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
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

      serviceReferences.forEach(sr -> registryEventSink.next(RegistryEvent.create(ADDED, sr)));
      registryEventSink.next(RegistryEvent.create(ADDED, serviceEndpoint));
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
          .forEach(sr -> registryEventSink.next(RegistryEvent.create(REMOVED, sr)));
      registryEventSink.next(RegistryEvent.create(REMOVED, serviceEndpoint));
    }

    return serviceEndpoint;
  }

  @Override
  public <T> Flux<RegistryEvent<T>> listen(Predicate<? super RegistryEvent<T>> predicate) {
    //noinspection unchecked
    return Flux.fromStream(
            Stream.concat(
                referencesByQualifier
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .map(sr -> (RegistryEvent) RegistryEvent.create(ADDED, sr)),
                serviceEndpoints
                    .values()
                    .stream()
                    .map(se -> (RegistryEvent) RegistryEvent.create(ADDED, se))))
        .concatWith(registryEvents)
        .filter((Predicate) predicate);
  }

  @Override
  public Mono<Void> close() {
    return Mono.create(
        sink -> {
          registryEventSink.complete();
          sink.success();
        });
  }
}
