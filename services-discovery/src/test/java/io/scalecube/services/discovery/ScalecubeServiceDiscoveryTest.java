package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_ADDED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type.ENDPOINT_REMOVED;
import static io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent.Type.ENDPOINT_ADDED_TO_GROUP;
import static io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent.Type.ENDPOINT_REMOVED_FROM_GROUP;
import static io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent.Type.GROUP_ADDED;
import static io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent.Type.GROUP_REMOVED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class ScalecubeServiceDiscoveryTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);
  public static final Duration SHORT_TIMEOUT = Duration.ofMillis(500);

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  @Test
  public void testEndpointIsAddedThenRemoved() {
    Address seedAddress = startSeed();

    AtomicInteger registeredCount = new AtomicInteger();
    AtomicInteger unregisteredCount = new AtomicInteger();

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> startServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(() -> startServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(() -> startServiceDiscovery(seedAddress));

    StepVerifier.create(Flux.merge(r1.discoveryEvents, r2.discoveryEvents, r3.discoveryEvents))
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_ADDED, event.type());
              assertNotNull(event.serviceEndpoint());
              return registeredCount.incrementAndGet() < 9;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .then(r3::shutdown)
        .thenConsumeWhile(
            event -> {
              assertEquals(ENDPOINT_REMOVED, event.type());
              assertNotNull(event.serviceEndpoint());
              return unregisteredCount.incrementAndGet() < 2;
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupIsAdded(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 3;

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));

    Stream.of(r1.groupDiscoveryEvents, r2.groupDiscoveryEvents, r3.groupDiscoveryEvents)
        .forEach(
            rp ->
                StepVerifier.create(rp)
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(
                        event -> {
                          assertEquals(GROUP_ADDED, event.type());
                          assertEquals(groupSize, event.groupSize());
                        })
                    .expectNoEvent(SHORT_TIMEOUT)
                    .thenCancel()
                    .verify());
  }

  @Test
  public void testSameGroupIdDifferentGroupSizes(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize_1 = 1;
    int groupSize_2 = 2;

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize_1));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize_2));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize_2));

    // Verify that group with groupSize_1 has been built
    Stream.of(r1.groupDiscoveryEvents)
        .forEach(
            rp ->
                StepVerifier.create(rp)
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(
                        event -> {
                          assertEquals(GROUP_ADDED, event.type());
                          assertEquals(groupSize_2, event.groupSize());
                        })
                    .expectNoEvent(SHORT_TIMEOUT)
                    .thenCancel()
                    .verify());

    // Verify that group with groupSize_2 has been built as well
    Stream.of(r2.groupDiscoveryEvents, r3.groupDiscoveryEvents)
        .forEach(
            rp ->
                StepVerifier.create(rp)
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(
                        event -> {
                          assertEquals(GROUP_ADDED, event.type());
                          assertThat(event.groupSize(), isOneOf(groupSize_1, groupSize_2));
                        })
                    .expectNoEvent(SHORT_TIMEOUT)
                    .thenCancel()
                    .verify());
  }

  @Test
  public void testSameGroupIdSameGroupSizeWithSeveralInstances(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 1;

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));

    // Verify that only one group with groupSize and groupId combination has been built
    Stream.of(r1.groupDiscoveryEvents, r2.groupDiscoveryEvents, r3.groupDiscoveryEvents)
        .forEach(
            rp ->
                StepVerifier.create(rp)
                    .expectSubscription()
                    .expectNoEvent(SHORT_TIMEOUT)
                    .thenCancel()
                    .verify());
  }

  @Test
  public void testSingleNodeGroupIsStillGroup(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 1; // group of size 1

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(() -> startServiceDiscovery(seedAddress));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));

    // Verify that group has been built and notified about that
    StepVerifier.create(Flux.merge(r1.groupDiscoveryEvents, r2.groupDiscoveryEvents))
        .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
        .assertNext(
            event -> {
              assertEquals(GROUP_ADDED, event.type());
              assertEquals(groupSize, event.groupSize());
            })
        .then(r2::shutdown)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED_FROM_GROUP, event.type()))
        .assertNext(
            event -> {
              assertEquals(GROUP_REMOVED, event.type());
              assertEquals(0, event.groupSize());
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupEventsOnBehalfOfNonGroupMember(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 2;

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));

    // Verify that Group under group id has been built
    Stream.of(r1.groupDiscoveryEvents, r2.groupDiscoveryEvents)
        .forEach(
            rp ->
                StepVerifier.create(rp)
                    .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
                    .assertNext(
                        event -> {
                          assertEquals(GROUP_ADDED, event.type());
                          assertEquals(groupSize, event.groupSize());
                        })
                    .expectNoEvent(SHORT_TIMEOUT)
                    .thenCancel()
                    .verify());

    // Verify registered/unregistered group events on non-group member
    StepVerifier.create(
            startServiceDiscovery(seedAddress) //
                .flatMapMany(ServiceDiscovery::listenGroupDiscovery))
        .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
        .assertNext(
            event -> {
              assertEquals(GROUP_ADDED, event.type());
              assertEquals(groupSize, event.groupSize());
            })
        .then(r1::shutdown)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED_FROM_GROUP, event.type()))
        .then(r2::shutdown)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED_FROM_GROUP, event.type()))
        .assertNext(
            event -> {
              assertEquals(GROUP_REMOVED, event.type());
              assertEquals(0, event.groupSize());
            })
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupIsNotRemovedAsLongAsOneIsLeft(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 3;

    RecordingServiceDiscovery r1 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r2 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));
    RecordingServiceDiscovery r3 =
        RecordingServiceDiscovery.create(
            () -> startServiceGroupDiscovery(seedAddress, groupId, groupSize));

    StepVerifier.create(r1.groupDiscoveryEvents)
        .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
        .assertNext(event -> assertEquals(ENDPOINT_ADDED_TO_GROUP, event.type()))
        .assertNext(
            event -> {
              assertEquals(GROUP_ADDED, event.type());
              assertEquals(groupSize, event.groupSize());
            })
        .then(r2::shutdown)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED_FROM_GROUP, event.type()))
        .then(r3::shutdown)
        .assertNext(event -> assertEquals(ENDPOINT_REMOVED_FROM_GROUP, event.type()))
        .expectNoEvent(SHORT_TIMEOUT)
        .thenCancel()
        .verify();
  }

  public static ServiceEndpoint newServiceEndpoint() {
    return ServiceEndpoint.builder().id(UUID.randomUUID().toString()).build();
  }

  public static ServiceEndpoint newServiceGroupEndpoint(String groupId, int groupSize) {
    return ServiceEndpoint.builder()
        .id(UUID.randomUUID().toString())
        .serviceGroup(groupId, groupSize)
        .build();
  }

  public static Address toAddress(Address address) {
    return Address.create(address.host(), address.port());
  }

  public Mono<ServiceDiscovery> startServiceGroupDiscovery(
      Address seedAddress, String groupId, int groupSize) {
    ClusterConfig clusterConfig =
        ClusterConfig.builder().seedMembers(toAddress(seedAddress)).build();
    ServiceEndpoint serviceEndpoint = newServiceGroupEndpoint(groupId, groupSize);
    return new ScalecubeServiceDiscovery(serviceEndpoint, clusterConfig).start();
  }

  private Mono<ServiceDiscovery> startServiceDiscovery(Address seedAddress) {
    ClusterConfig clusterConfig =
        ClusterConfig.builder().seedMembers(toAddress(seedAddress)).build();
    ServiceEndpoint serviceEndpoint = newServiceEndpoint();
    return new ScalecubeServiceDiscovery(serviceEndpoint, clusterConfig).start();
  }

  private Address startSeed() {
    return new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block().address();
  }

  private static class RecordingServiceDiscovery {

    final ReplayProcessor<ServiceDiscovery> instance = ReplayProcessor.create();
    final ReplayProcessor<ServiceDiscoveryEvent> discoveryEvents = ReplayProcessor.create();
    final ReplayProcessor<ServiceGroupDiscoveryEvent> groupDiscoveryEvents =
        ReplayProcessor.create();

    static RecordingServiceDiscovery create(Supplier<Mono<ServiceDiscovery>> supplier) {
      RecordingServiceDiscovery result = new RecordingServiceDiscovery();
      supplier
          .get()
          .doOnNext(result.instance::onNext)
          .subscribe(
              sd -> {
                sd.listenDiscovery().subscribe(result.discoveryEvents);
                sd.listenGroupDiscovery().subscribe(result.groupDiscoveryEvents);
              });
      return result;
    }

    void shutdown() {
      instance.flatMap(ServiceDiscovery::shutdown).then().subscribe();
    }
  }
}
