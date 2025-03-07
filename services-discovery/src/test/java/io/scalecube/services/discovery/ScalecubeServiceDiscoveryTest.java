package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_ADDED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.codec.jackson.JacksonMetadataCodec;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.JdkMetadataCodec;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

class ScalecubeServiceDiscoveryTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);
  public static final Duration SHORT_TIMEOUT = Duration.ofMillis(500);
  public static final AtomicInteger ID_COUNTER = new AtomicInteger();
  public static final GossipConfig GOSSIP_CONFIG = GossipConfig.defaultLocalConfig();
  public static final FailureDetectorConfig FAILURE_DETECTOR_CONFIG =
      FailureDetectorConfig.defaultLocalConfig();
  public static final MembershipConfig MEMBERSHIP_CONFIG = MembershipConfig.defaultLocalConfig();
  public static final int CLUSTER_SIZE = 3 + 1; // r1 + r2 + r3 (plus 1 for be sure)
  public static final Address SEED_ADDRESS = Address.from("localhost:5678");

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  private static Stream<Arguments> metadataCodecSource() {
    return Stream.of(
        Arguments.of(new JdkMetadataCodec()), Arguments.of(new JacksonMetadataCodec()));
  }

  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void testMetadataCodec(MetadataCodec metadataCodec) {
    ServiceEndpoint serviceEndpoint =
        ServiceEndpoint.builder()
            .id(UUID.randomUUID().toString())
            .tags(Collections.singletonMap("K", "V"))
            .contentTypes(Collections.singleton("json"))
            .appendServiceRegistrations(
                Collections.singletonList(
                    new ServiceRegistration(
                        "namespace",
                        Collections.singletonMap("KK", "VV"),
                        Collections.singletonList(
                            ServiceMethodDefinition.builder()
                                .action("action0")
                                .tags(Collections.singletonMap("KKK0", "VVV"))
                                .secured(true)
                                .allowedRoles(List.of("read", "write"))
                                .build()))))
            .appendServiceRegistrations(
                Collections.singletonList(
                    new ServiceRegistration(
                        "namespace",
                        Collections.singletonMap("KK", "VV"),
                        Collections.singletonList(
                            ServiceMethodDefinition.builder()
                                .action("action1")
                                .tags(Collections.singletonMap("KKK1", "VVV"))
                                .secured(true)
                                .allowedRoles(List.of("replay", "archive"))
                                .build()))))
            .appendServiceRegistrations(
                Collections.singletonList(
                    new ServiceRegistration(
                        "namespace",
                        Collections.singletonMap("KK", "VV"),
                        Collections.singletonList(
                            ServiceMethodDefinition.builder()
                                .action("action2")
                                .tags(Collections.singletonMap("KKK2", "VVV"))
                                .secured(true)
                                .build()))))
            .build();

    ByteBuffer buffer = metadataCodec.serialize(serviceEndpoint);
    ServiceEndpoint serviceEndpoint1 = (ServiceEndpoint) metadataCodec.deserialize(buffer);
    Assertions.assertEquals(serviceEndpoint.id(), serviceEndpoint1.id());
    Assertions.assertEquals(1, serviceEndpoint1.tags().size());
    Assertions.assertEquals(1, serviceEndpoint1.contentTypes().size());

    List<ServiceRegistration> serviceRegistrations =
        new ArrayList<>(serviceEndpoint1.serviceRegistrations());
    Assertions.assertEquals(3, serviceRegistrations.size());
    for (ServiceRegistration serviceRegistration : serviceRegistrations) {
      Assertions.assertEquals(1, serviceRegistration.methods().size());
      Assertions.assertEquals(1, serviceRegistration.tags().size());
    }
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void testEndpointIsAddedThenRemoved(MetadataCodec metadataCodec) {
    startSeed(metadataCodec);

    AtomicInteger registeredCount = new AtomicInteger();
    AtomicInteger unregisteredCount = new AtomicInteger();

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));

    int expectedAddedEventsNum = 9; // (1+3)x(1+3) - (1+3)/*exclude self*/ - 3/*exclude seed*/
    int expectedRemovedEventsNum = 2; // r3 is shutdown => await by 1 event on r1 and r2

    StepVerifier.create(
            Flux.merge(
                r1.nonGroupDiscoveryEvents(),
                r2.nonGroupDiscoveryEvents(),
                r3.nonGroupDiscoveryEvents()))
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_ADDED, event.type());
              assertNotNull(event.serviceEndpoint());
              return registeredCount.incrementAndGet() < expectedAddedEventsNum;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .then(r3::shutdown)
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_REMOVED, event.type());
              assertNotNull(event.serviceEndpoint());
              return unregisteredCount.incrementAndGet() < expectedRemovedEventsNum;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("metadataCodecSource")
  public void testEndpointIsRestarted(MetadataCodec metadataCodec) {
    startSeed(metadataCodec);

    AtomicInteger registeredCount = new AtomicInteger();
    AtomicInteger unregisteredCount = new AtomicInteger();

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(SEED_ADDRESS, metadataCodec));

    int expectedAddedEventsNum = 9; // (1+3)x(1+3) - (1+3)/*exclude self*/ - 3/*exclude seed*/
    int expectedRemovedEventsNum = 2; // r3 is shutdown => await by 1 event on r1 and r2

    StepVerifier.create(
            Flux.merge(
                r1.nonGroupDiscoveryEvents(),
                r2.nonGroupDiscoveryEvents(),
                r3.nonGroupDiscoveryEvents()))
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_ADDED, event.type());
              assertNotNull(event.serviceEndpoint());
              return registeredCount.incrementAndGet() < expectedAddedEventsNum;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .then(r3::shutdown)
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_REMOVED, event.type());
              assertNotNull(event.serviceEndpoint());
              return unregisteredCount.incrementAndGet() < expectedRemovedEventsNum;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();

    AtomicInteger registeredCountAfterRestart = new AtomicInteger();
    int expectedAddedEventsNumAfterRestart = 2; // r3 is restared => await by 1 event on r1 and r2

    r1 = r1.resubscribe();
    r2 = r2.resubscribe();
    r3 = r3.recreate();

    StepVerifier.create(
            Flux.merge(
                r1.nonGroupDiscoveryEvents(),
                r2.nonGroupDiscoveryEvents(),
                r3.nonGroupDiscoveryEvents()))
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_ADDED, event.type());
              assertNotNull(event.serviceEndpoint());
              return registeredCountAfterRestart.incrementAndGet()
                  < expectedAddedEventsNumAfterRestart;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  public static ServiceEndpoint newServiceEndpoint() {
    return ServiceEndpoint.builder().id("" + ID_COUNTER.incrementAndGet()).build();
  }

  private Mono<ServiceDiscovery> newServiceDiscovery(
      Address seedAddress, MetadataCodec metadataCodec) {
    return Mono.fromCallable(
        () ->
            new ScalecubeServiceDiscovery()
                .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                .options(opts -> opts.metadata(newServiceEndpoint()))
                .options(opts -> opts.metadataCodec(metadataCodec))
                .gossip(cfg -> GOSSIP_CONFIG)
                .failureDetector(cfg -> FAILURE_DETECTOR_CONFIG)
                .membership(cfg -> MEMBERSHIP_CONFIG)
                .membership(cfg -> cfg.seedMembers(seedAddress.toString())));
  }

  private void startSeed(MetadataCodec metadataCodec) {
    new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .membership(opts -> opts.seedMembers(SEED_ADDRESS.toString()))
        .options(opts -> opts.metadata(newServiceEndpoint()))
        .options(opts -> opts.metadataCodec(metadataCodec))
        .gossip(cfg -> GOSSIP_CONFIG)
        .failureDetector(cfg -> FAILURE_DETECTOR_CONFIG)
        .membership(cfg -> MEMBERSHIP_CONFIG)
        .start();
  }

  private static class RecordingServiceDiscovery {

    final Supplier<Mono<ServiceDiscovery>> supplier;
    final Sinks.Many<ServiceDiscoveryEvent> sink = Sinks.many().replay().all();

    ServiceDiscovery serviceDiscovery; // effectively final

    private RecordingServiceDiscovery(Supplier<Mono<ServiceDiscovery>> supplier) {
      this.supplier = supplier;
    }

    private RecordingServiceDiscovery(RecordingServiceDiscovery other) {
      this.serviceDiscovery = other.serviceDiscovery;
      this.supplier = other.supplier;
    }

    Flux<ServiceDiscoveryEvent> nonGroupDiscoveryEvents() {
      return sink.asFlux()
          .onBackpressureBuffer()
          .filter(ScalecubeServiceDiscoveryTest::filterNonGroupDiscoveryEvents);
    }

    RecordingServiceDiscovery resubscribe() {
      return new RecordingServiceDiscovery(this).subscribe();
    }

    RecordingServiceDiscovery recreate() {
      return create(supplier);
    }

    static RecordingServiceDiscovery create(Supplier<Mono<ServiceDiscovery>> supplier) {
      RecordingServiceDiscovery result = new RecordingServiceDiscovery(supplier);
      Mono<ServiceDiscovery> serviceDiscoveryMono = supplier.get();
      serviceDiscoveryMono.subscribe(
          serviceDiscovery -> {
            result.serviceDiscovery = serviceDiscovery;
            result.subscribe();
            result.serviceDiscovery.start();
          });
      return result;
    }

    private RecordingServiceDiscovery subscribe() {
      serviceDiscovery
          .listen()
          .subscribe(sink::tryEmitNext, sink::tryEmitError, sink::tryEmitComplete);
      return this;
    }

    RecordingServiceDiscovery shutdown() {
      int pingInterval = FailureDetectorConfig.defaultLocalConfig().pingInterval();
      long timeout =
          ClusterMath.suspicionTimeout(
              MEMBERSHIP_CONFIG.suspicionMult(), CLUSTER_SIZE, pingInterval);
      serviceDiscovery.shutdown();
      Mono.delay(Duration.ofMillis(timeout)).block();
      return this;
    }
  }

  private static boolean filterNonGroupDiscoveryEvents(ServiceDiscoveryEvent event) {
    return event.isEndpointAdded() || event.isEndpointRemoved();
  }
}
