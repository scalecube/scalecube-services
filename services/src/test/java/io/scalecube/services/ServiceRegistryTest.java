package io.scalecube.services;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_ADDED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_LEAVING;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.sut.AnnotationService;
import io.scalecube.services.sut.AnnotationServiceImpl;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

public class ServiceRegistryTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(6);

  @Test
  public void test_added_removed_registration_events() {

    List<ServiceDiscoveryEvent> events = new ArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    seed.discovery().listenDiscovery().subscribe(events::add);

    Address seedAddress = seed.discovery().address();

    Microservices ms1 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();

    Mono.when(ms1.shutdown(), ms2.shutdown()).then(Mono.delay(TIMEOUT)).block();

    assertEquals(6, events.size());
    assertEquals(ENDPOINT_ADDED, events.get(0).type());
    assertEquals(ENDPOINT_ADDED, events.get(1).type());
    assertEquals(ENDPOINT_LEAVING, events.get(2).type());
    assertEquals(ENDPOINT_LEAVING, events.get(3).type());
    assertEquals(ENDPOINT_REMOVED, events.get(4).type());
    assertEquals(ENDPOINT_REMOVED, events.get(5).type());

    seed.shutdown().block();
  }

  private static ServiceDiscovery serviceDiscovery(
      ServiceEndpoint serviceEndpoint, Address address) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(opts -> opts.membership(cfg -> cfg.seedMembers(address)));
  }

  @Test
  public void test_listen_to_discovery_events() {
    ReplayProcessor<ServiceDiscoveryEvent> processor = ReplayProcessor.create();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
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
                      .transport(RSocketServiceTransport::new)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(RSocketServiceTransport::new)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms2);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(() -> cluster.remove(2).shutdown().block())
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
        .thenAwait(TIMEOUT)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED, event.type()))
        .then(() -> cluster.remove(1).shutdown().block())
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
        .thenAwait(TIMEOUT)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED, event.type()))
        .thenCancel()
        .verify(TIMEOUT);

    StepVerifier.create(seed.call().api(AnnotationService.class).serviceDiscoveryEventTypes())
        .assertNext(type -> assertEquals(ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(ENDPOINT_LEAVING, type))
        .assertNext(type -> assertEquals(ENDPOINT_REMOVED, type))
        .assertNext(type -> assertEquals(ENDPOINT_LEAVING, type))
        .assertNext(type -> assertEquals(ENDPOINT_REMOVED, type))
        .thenCancel()
        .verify(TIMEOUT);

    Mono.when(cluster.stream().map(Microservices::shutdown).toArray(Mono[]::new))
        .then(Mono.delay(TIMEOUT))
        .block();
  }

  @Test
  public void test_delayed_listen_to_discovery_events() {
    ReplayProcessor<ServiceDiscoveryEvent> processor = ReplayProcessor.create();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
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
                      .transport(RSocketServiceTransport::new)
                      .services(new GreetingServiceImpl(), new AnnotationServiceImpl())
                      .startAwait();
              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.builder()
                      .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedAddress))
                      .transport(RSocketServiceTransport::new)
                      .services(new GreetingServiceImpl())
                      .startAwait();
              cluster.add(ms2);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .thenCancel()
        .verify(TIMEOUT);

    StepVerifier.create(seed.call().api(AnnotationService.class).serviceDiscoveryEventTypes())
        .assertNext(type -> assertEquals(ENDPOINT_ADDED, type))
        .assertNext(type -> assertEquals(ENDPOINT_ADDED, type))
        .thenCancel()
        .verify(TIMEOUT);

    Mono.when(cluster.stream().map(Microservices::shutdown).toArray(Mono[]::new))
        .then(Mono.delay(TIMEOUT))
        .block();
  }
}
