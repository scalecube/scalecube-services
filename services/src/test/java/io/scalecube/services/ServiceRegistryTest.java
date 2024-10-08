package io.scalecube.services;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_ADDED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_LEAVING;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.cluster.codec.jackson.JacksonMetadataCodec;
import io.scalecube.cluster.metadata.JdkMetadataCodec;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.sut.AnnotationService;
import io.scalecube.services.sut.AnnotationServiceImpl;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class ServiceRegistryTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(30);

  public static Stream<Arguments> metadataCodecSource() {
    return Stream.of(
        Arguments.of(new JdkMetadataCodec()), Arguments.of(new JacksonMetadataCodec()));
  }

  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void test_added_removed_registration_events(MetadataCodec metadataCodec) {
    Sinks.Many<ServiceDiscoveryEvent> events = Sinks.many().replay().all();

    Microservices seed =
        Microservices.start(
            new Context()
                .discovery(defServiceDiscovery(metadataCodec))
                .transport(RSocketServiceTransport::new));

    seed.listenDiscovery()
        .subscribe(events::tryEmitNext, events::tryEmitError, events::tryEmitComplete);

    Address seedAddress = seed.discoveryAddress();

    Microservices ms1 =
        Microservices.start(
            new Context()
                .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                .transport(RSocketServiceTransport::new)
                .services(new GreetingServiceImpl()));

    Microservices ms2 =
        Microservices.start(
            new Context()
                .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                .transport(RSocketServiceTransport::new)
                .services(new GreetingServiceImpl()));

    StepVerifier.create(events.asFlux().onBackpressureBuffer())
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              ms1.close();
              ms2.close();
            })
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED, event.type()))
        .then(seed::close)
        .thenCancel()
        .verify(TIMEOUT);
  }

  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void test_listen_to_discovery_events(MetadataCodec metadataCodec) {
    Sinks.Many<ServiceDiscoveryEvent> processor = Sinks.many().replay().all();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.start(
            new Context()
                .discovery(defServiceDiscovery(metadataCodec))
                .transport(RSocketServiceTransport::new)
                .services(new AnnotationServiceImpl()));

    cluster.add(seed);

    seed.listenDiscovery()
        .subscribe(processor::tryEmitNext, processor::tryEmitError, processor::tryEmitComplete);

    Address seedAddress = seed.discoveryAddress();

    StepVerifier.create(processor.asFlux().onBackpressureBuffer())
        .then(
            () -> {
              Microservices ms1 =
                  Microservices.start(
                      new Context()
                          .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                          .transport(RSocketServiceTransport::new)
                          .services(new GreetingServiceImpl()));

              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.start(
                      new Context()
                          .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                          .transport(RSocketServiceTransport::new)
                          .services(new GreetingServiceImpl()));

              cluster.add(ms2);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(() -> cluster.remove(2).close())
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED, event.type()))
        .then(() -> cluster.remove(1).close())
        .assertNext(event -> assertEquals(ENDPOINT_LEAVING, event.type()))
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

    cluster.forEach(Microservices::close);
  }

  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void test_delayed_listen_to_discovery_events(MetadataCodec metadataCodec) {
    Sinks.Many<ServiceDiscoveryEvent> processor = Sinks.many().replay().all();

    List<Microservices> cluster = new CopyOnWriteArrayList<>();

    Microservices seed =
        Microservices.start(
            new Context()
                .discovery(defServiceDiscovery(metadataCodec))
                .transport(RSocketServiceTransport::new)
                .services(new GreetingServiceImpl()));
    cluster.add(seed);

    seed.listenDiscovery()
        .subscribe(processor::tryEmitNext, processor::tryEmitError, processor::tryEmitComplete);

    Address seedAddress = seed.discoveryAddress();

    StepVerifier.create(processor.asFlux().onBackpressureBuffer())
        .then(
            () -> {
              Microservices ms1 =
                  Microservices.start(
                      new Context()
                          .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                          .transport(RSocketServiceTransport::new)
                          .services(new GreetingServiceImpl(), new AnnotationServiceImpl()));
              cluster.add(ms1);
            })
        .assertNext(event -> assertEquals(ENDPOINT_ADDED, event.type()))
        .then(
            () -> {
              Microservices ms2 =
                  Microservices.start(
                      new Context()
                          .discovery(defServiceDiscovery(seedAddress, metadataCodec))
                          .transport(RSocketServiceTransport::new)
                          .services(new GreetingServiceImpl()));

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

    cluster.forEach(Microservices::close);
  }

  private ServiceDiscoveryFactory defServiceDiscovery(MetadataCodec metadataCodec) {
    return endpoint ->
        new ScalecubeServiceDiscovery()
            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
            .options(opts -> opts.metadata(endpoint))
            .options(cfg -> cfg.metadataCodec(metadataCodec));
  }

  private static ServiceDiscoveryFactory defServiceDiscovery(
      Address address, MetadataCodec metadataCodec) {
    return endpoint ->
        new ScalecubeServiceDiscovery()
            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
            .options(opts -> opts.metadata(endpoint))
            .options(cfg -> cfg.metadataCodec(metadataCodec))
            .membership(cfg -> cfg.seedMembers(address.toString()));
  }
}
