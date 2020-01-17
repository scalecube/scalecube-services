package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_ADDED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.JdkMetadataCodec;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class ScalecubeServiceDiscoveryTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);
  public static final Duration SHORT_TIMEOUT = Duration.ofMillis(500);
  public static final AtomicInteger ID_COUNTER = new AtomicInteger();
  public static final GossipConfig GOSSIP_CONFIG = GossipConfig.defaultLocalConfig();
  public static final MembershipConfig MEMBERSHIP_CONFIG = MembershipConfig.defaultLocalConfig();
  public static final int CLUSTER_SIZE = 3 + 1; // r1 + r2 + r3 (plus 1 for be sure)

  private JdkMetadataCodec jdkMetadataCodec = new JdkMetadataCodec();

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  @Test
  public void testJdkMetadataCodec() {
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
                            new ServiceMethodDefinition(
                                "action0", Collections.singletonMap("KKK0", "VVV"), true)))))
            .appendServiceRegistrations(
                Collections.singletonList(
                    new ServiceRegistration(
                        "namespace",
                        Collections.singletonMap("KK", "VV"),
                        Collections.singletonList(
                            new ServiceMethodDefinition(
                                "action1", Collections.singletonMap("KKK1", "VVV"), true)))))
            .appendServiceRegistrations(
                Collections.singletonList(
                    new ServiceRegistration(
                        "namespace",
                        Collections.singletonMap("KK", "VV"),
                        Collections.singletonList(
                            new ServiceMethodDefinition(
                                "action2", Collections.singletonMap("KKK2", "VVV"), true)))))
            .build();

    ByteBuffer buffer = jdkMetadataCodec.serialize(serviceEndpoint);
    ServiceEndpoint serviceEndpoint1 = (ServiceEndpoint) jdkMetadataCodec.deserialize(buffer);
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

  @Test
  public void testEndpointIsAddedThenRemoved() {
    Address seedAddress = startSeed();

    AtomicInteger registeredCount = new AtomicInteger();
    AtomicInteger unregisteredCount = new AtomicInteger();

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));

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

  @Test
  public void testEndpointIsRestarted() {
    Address seedAddress = startSeed();

    AtomicInteger registeredCount = new AtomicInteger();
    AtomicInteger unregisteredCount = new AtomicInteger();

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(() -> newServiceDiscovery(seedAddress));

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

  private Mono<ServiceDiscovery> newServiceDiscovery(Address seedAddress) {
    return Mono.fromCallable(
        () -> {
          ServiceEndpoint serviceEndpoint = newServiceEndpoint();
          return new ScalecubeServiceDiscovery(serviceEndpoint)
              .options(opts -> opts.gossip(cfg -> GOSSIP_CONFIG))
              .options(opts -> opts.membership(cfg -> MEMBERSHIP_CONFIG))
              .options(opts -> opts.membership(cfg -> cfg.seedMembers(seedAddress)));
        });
  }

  private Address startSeed() {
    return new ScalecubeServiceDiscovery(newServiceEndpoint())
        .options(opts -> opts.gossip(cfg -> GOSSIP_CONFIG))
        .options(opts -> opts.membership(cfg -> MEMBERSHIP_CONFIG))
        .start()
        .block()
        .address();
  }

  private static class RecordingServiceDiscovery {

    final Supplier<Mono<ServiceDiscovery>> supplier;
    final ReplayProcessor<ServiceDiscoveryEvent> discoveryEvents = ReplayProcessor.create();

    ServiceDiscovery serviceDiscovery; // effectively final

    private RecordingServiceDiscovery(Supplier<Mono<ServiceDiscovery>> supplier) {
      this.supplier = supplier;
    }

    private RecordingServiceDiscovery(RecordingServiceDiscovery other) {
      this.serviceDiscovery = other.serviceDiscovery;
      this.supplier = other.supplier;
    }

    Flux<ServiceDiscoveryEvent> nonGroupDiscoveryEvents() {
      return discoveryEvents.filter(ScalecubeServiceDiscoveryTest::filterNonGroupDiscoveryEvents);
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
            result.serviceDiscovery.start().block();
          });
      return result;
    }

    private RecordingServiceDiscovery subscribe() {
      serviceDiscovery.listenDiscovery().subscribe(discoveryEvents);
      return this;
    }

    RecordingServiceDiscovery shutdown() {
      int pingInterval = FailureDetectorConfig.defaultLocalConfig().pingInterval();
      long timeout =
          ClusterMath.suspicionTimeout(
              MEMBERSHIP_CONFIG.suspicionMult(), CLUSTER_SIZE, pingInterval);
      serviceDiscovery.shutdown().then(Mono.delay(Duration.ofMillis(timeout))).block();
      return this;
    }
  }

  private static boolean filterNonGroupDiscoveryEvents(ServiceDiscoveryEvent event) {
    return event.isEndpointAdded() || event.isEndpointRemoved();
  }
}
