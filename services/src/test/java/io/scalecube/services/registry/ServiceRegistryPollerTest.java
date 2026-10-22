package io.scalecube.services.registry;

import static io.scalecube.services.transport.jackson.JacksonCodec.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.registry.api.ServiceRegistry.Poller;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ServiceRegistryPollerTest {

  private final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
  private final List<String> added = new ArrayList<>();
  private final List<String> removed = new ArrayList<>();

  @Test
  void testVersionChangesOnlyWhenEndpointsChange() {
    assertEquals(0, serviceRegistry.version());

    serviceRegistry.registerService(endpoint("a"));
    assertEquals(1, serviceRegistry.version());

    serviceRegistry.registerService(endpoint("a")); // already registered
    serviceRegistry.unregisterService("unknown");
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new HelloImpl())
            .errorMapper(mock(ServiceProviderErrorMapper.class))
            .dataDecoder(mock(ServiceMessageDataDecoder.class))
            .build()); // local service, not an endpoint
    assertEquals(1, serviceRegistry.version());

    serviceRegistry.unregisterService("a");
    assertEquals(2, serviceRegistry.version());
  }

  @Test
  void testFirstPollReportsRegisteredEndpoints() {
    serviceRegistry.registerService(endpoint("a"));
    serviceRegistry.registerService(endpoint("b"));

    final var poller = serviceRegistry.poller();

    assertEquals(2, poll(poller));
    assertEquals(Set.of("a", "b"), Set.copyOf(added));
    assertEquals(List.of(), removed);
  }

  @Test
  void testPollReportsAddedAndRemoved() {
    final var poller = serviceRegistry.poller();
    assertEquals(0, poll(poller), "nothing registered");

    serviceRegistry.registerService(endpoint("a"));
    serviceRegistry.registerService(endpoint("b"));
    assertEquals(2, poll(poller));
    assertEquals(Set.of("a", "b"), Set.copyOf(added));
    assertEquals(List.of(), removed);

    assertEquals(0, poll(poller), "no changes");
    assertEquals(List.of(), added);
    assertEquals(List.of(), removed);

    serviceRegistry.unregisterService("a");
    serviceRegistry.registerService(endpoint("c"));
    assertEquals(2, poll(poller));
    assertEquals(List.of("c"), added);
    assertEquals(List.of("a"), removed);

    serviceRegistry.registerService(endpoint("d"));
    serviceRegistry.unregisterService("d");
    assertEquals(0, poll(poller), "added and removed between polls: no change");
  }

  @Test
  void testEndpointsAreUpdatedWhileCallbacksRun() {
    final var poller = serviceRegistry.poller();
    serviceRegistry.registerService(endpoint("a"));
    serviceRegistry.registerService(endpoint("b"));
    poll(poller);

    serviceRegistry.unregisterService("a");
    final var seenInCallback = new ArrayList<Set<String>>();
    poller.poll(endpoint -> {}, endpoint -> seenInCallback.add(ids(poller)));

    assertEquals(List.of(Set.of("b")), seenInCallback);
    assertEquals(Set.of("b"), ids(poller));
  }

  @Test
  void testPollersAreIndependent() {
    final var first = serviceRegistry.poller();
    final var second = serviceRegistry.poller();
    serviceRegistry.registerService(endpoint("a"));

    assertEquals(1, poll(first));
    assertEquals(1, poll(second));
    assertEquals(0, poll(first));
  }

  @Test
  void testPollerConvergesUnderConcurrentWriter() throws Exception {
    final var poller = serviceRegistry.poller();
    final var mirror = new HashSet<String>(); // built only from the reported changes
    final var writerDone = new AtomicBoolean();

    final var writer =
        new Thread(
            () -> {
              final var random = new Random(42);
              for (int i = 0; i < 100_000; i++) {
                final var id = "e" + random.nextInt(32);
                if (random.nextBoolean()) {
                  serviceRegistry.registerService(endpoint(id));
                } else {
                  serviceRegistry.unregisterService(id);
                }
              }
              writerDone.set(true);
            });
    writer.start();

    while (!writerDone.get()) {
      poller.poll(endpoint -> mirror.add(endpoint.id()), endpoint -> mirror.remove(endpoint.id()));
    }
    writer.join();
    poller.poll(endpoint -> mirror.add(endpoint.id()), endpoint -> mirror.remove(endpoint.id()));

    assertEquals(ids(serviceRegistry.listServiceEndpoints()), mirror);
    assertEquals(mirror, ids(poller));
  }

  private int poll(Poller poller) {
    added.clear();
    removed.clear();
    return poller.poll(endpoint -> added.add(endpoint.id()), endpoint -> removed.add(endpoint.id()));
  }

  private static Set<String> ids(Poller poller) {
    return ids(poller.endpoints());
  }

  private static Set<String> ids(Collection<ServiceEndpoint> endpoints) {
    return endpoints.stream().map(ServiceEndpoint::id).collect(Collectors.toSet());
  }

  private static ServiceEndpoint endpoint(String id) {
    return ServiceEndpoint.builder()
        .id(id)
        .name(id)
        .address(Address.create(id, 4848))
        .contentTypes(Set.of(CONTENT_TYPE))
        .serviceRegistrations(List.of())
        .build();
  }

  @Service("hello")
  public interface Hello {

    @ServiceMethod
    Mono<String> hello();
  }

  public static class HelloImpl implements Hello {

    @Override
    public Mono<String> hello() {
      return Mono.just("hello");
    }
  }
}
