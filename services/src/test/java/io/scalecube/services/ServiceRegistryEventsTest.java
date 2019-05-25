package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.ClusterAddresses;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.sut.AnnotationService;
import io.scalecube.services.sut.AnnotationServiceImpl;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.api.Address;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<ServiceDiscoveryEvent> events = new ArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .startAwait();

    seed.discovery().listenDiscovery().subscribe(events::add);

    Address seedAddress = seed.discovery().address();

    Microservices ms1 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new GreetingServiceImpl())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new GreetingServiceImpl())
            .startAwait();

    Mono.when(ms1.shutdown(), ms2.shutdown()).block(Duration.ofSeconds(6));

    assertEquals(4, events.size());
    assertEquals(Type.ENDPOINT_ADDED, events.get(0).type());
    assertEquals(Type.ENDPOINT_ADDED, events.get(1).type());
    assertEquals(Type.ENDPOINT_REMOVED, events.get(2).type());
    assertEquals(Type.ENDPOINT_REMOVED, events.get(3).type());

    seed.shutdown().block(Duration.ofSeconds(6));
  }

  private static ServiceDiscovery serviceDiscovery(
      ServiceEndpoint serviceEndpoint, Address address) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(opts -> opts.seedMembers(ClusterAddresses.toAddress(address)));
  }

  @Test
  public void test_listen_to_discovery_events() {
    ReplayProcessor<ServiceDiscoveryEvent> processor = ReplayProcessor.create();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new AnnotationServiceImpl())
            .startAwait();
    cluster.add(seed);

    seed.discovery().listenDiscovery().subscribe(processor);

    Address seedAddress = seed.discovery().address();

    StepVerifier.create(processor)
        .then(
            () -> {
              Microservices ms1 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(ServiceTransports::rsocketServiceTransport)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(ServiceTransports::rsocketServiceTransport)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms2);
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 = cluster.remove(2);
              ms2.shutdown().subscribe();
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_REMOVED, event.type()))
        .then(
            () -> {
              Microservices ms1 = cluster.remove(1);
              ms1.shutdown().subscribe();
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_REMOVED, event.type()))
        .thenCancel()
        .verify(Duration.ofSeconds(6));

    StepVerifier.create(seed.call().api(AnnotationService.class).serviceDiscoveryEventTypes())
        .assertNext(type -> assertEquals(Type.ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(Type.ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(Type.ENDPOINT_REMOVED, type))
        .assertNext(type -> assertEquals(Type.ENDPOINT_REMOVED, type))
        .thenCancel()
        .verify(Duration.ofSeconds(6));

    Mono.when(cluster.stream().map(Microservices::shutdown).toArray(Mono[]::new))
        .block(Duration.ofSeconds(6));
  }

  @Test
  public void test_delayed_listen_to_discovery_events() {
    ReplayProcessor<ServiceDiscoveryEvent> processor = ReplayProcessor.create();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new GreetingServiceImpl())
            .startAwait();
    cluster.add(seed);

    seed.discovery().listenDiscovery().subscribe(processor);

    Address seedAddress = seed.discovery().address();

    StepVerifier.create(processor)
        .then(
            () -> {
              Microservices ms1 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(ServiceTransports::rsocketServiceTransport)
                      .services(new GreetingServiceImpl(), new AnnotationServiceImpl())
                      .startAwait();
              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(ServiceTransports::rsocketServiceTransport)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms2);
            })
        .assertNext(event -> assertEquals(Type.ENDPOINT_ADDED, event.type()))
        .thenCancel()
        .verify(Duration.ofSeconds(6));

    StepVerifier.create(seed.call().api(AnnotationService.class).serviceDiscoveryEventTypes())
        .assertNext(type -> assertEquals(Type.ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(Type.ENDPOINT_ADDED, type))
        .thenCancel()
        .verify(Duration.ofSeconds(6));

    Mono.when(cluster.stream().map(Microservices::shutdown).toArray(Mono[]::new))
        .block(Duration.ofSeconds(6));
  }
}
