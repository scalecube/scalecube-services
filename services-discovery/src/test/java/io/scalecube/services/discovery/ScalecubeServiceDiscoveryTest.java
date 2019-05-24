package io.scalecube.services.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceGroupDiscoveryEvent;
import io.scalecube.services.transport.api.Address;
import java.time.Duration;
import java.util.UUID;
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

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  @Test
  public void testGroupIsRegistered(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 3;
    ReplayProcessor<ServiceGroupDiscoveryEvent> rp1 = ReplayProcessor.create();
    ReplayProcessor<ServiceGroupDiscoveryEvent> rp2 = ReplayProcessor.create();
    ReplayProcessor<ServiceGroupDiscoveryEvent> rp3 = ReplayProcessor.create();

    // Verify that Group under group id has been built
    StepVerifier.create(
            Flux.merge(
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)
                    .doOnNext(rp1::onNext), //
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)
                    .doOnNext(rp2::onNext),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)
                    .doOnNext(rp3::onNext)))
        .expectNextCount(9)
        .expectNoEvent(Duration.ofSeconds(1))
        .thenCancel()
        .verify();

    Stream.of(rp1, rp2, rp3)
        .forEach(
            rp ->
                StepVerifier.create(Flux.concat(rp))
                    .assertNext(event -> assertTrue(event.isEndpointAddedToTheGroup()))
                    .assertNext(event -> assertTrue(event.isEndpointAddedToTheGroup()))
                    .assertNext(
                        event -> {
                          assertTrue(event.isGroupRegistered());
                          assertEquals(groupSize, event.groupSize());
                        })
                    .expectNoEvent(Duration.ofSeconds(1))
                    .thenCancel()
                    .verify());
  }

  @Test
  public void testRegisterMoreThanDeclaredInTheGroup(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize_1 = 1;
    int groupSize_2 = 2;
    // Verify that several groups under same group id have been built
    StepVerifier.create(
            Flux.merge(
                startServiceGroupDiscovery(seedAddress, groupId, groupSize_1)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize_2)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize_2)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)))
        .expectNextMatches(
            event ->
                event.isGroupRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isGroupRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isGroupRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isGroupRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .thenCancel()
        .verify();
  }

  @Test
  public void testSingleNodeGroupIsStillGroup(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 1; // group of size 1

    // Verify that group has been built and notified about that
    ReplayProcessor<ServiceDiscovery> startedServiceDiscoveries = ReplayProcessor.create(1);
    StepVerifier.create(
            Flux.merge(
                startServiceDiscovery(seedAddress) //
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedServiceDiscoveries::onNext)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)))
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedServiceDiscoveries.flatMap(ServiceDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isGroupUnregistered() && event.groupSize() == 0)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupEventsOnBehalfOfNonGroupMember(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 2;
    ReplayProcessor<ServiceDiscovery> startedServiceDiscoveries = ReplayProcessor.create(groupSize);
    // Verify that Group under group id has been built
    StepVerifier.create(
            Flux.merge(
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)))
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .thenCancel()
        .verify();

    // Verify registered/unregistered group events on non-group member
    StepVerifier.create(
            startServiceDiscovery(seedAddress) //
                .flatMapMany(ServiceDiscovery::listenGroupDiscovery))
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedServiceDiscoveries.flatMap(ServiceDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isGroupUnregistered() && event.groupSize() == 0)
        .thenCancel()
        .verify();
  }

  @Test
  public void testLastOneInTheGroupStillReceiveEvents(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    int groupSize = 3;
    ReplayProcessor<ServiceDiscovery> startedServiceDiscoveries = ReplayProcessor.create();
    // Verify that Group under group id has been built
    StepVerifier.create(
            Flux.merge(
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery), // dont track started
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)))
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isGroupRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedServiceDiscoveries.flatMap(ServiceDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isGroupUnregistered() && event.groupSize() == 0)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupSizeDefinition(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    Address seedAddress = startSeed();

    ReplayProcessor<ServiceDiscovery> startedServiceDiscoveries = ReplayProcessor.create();
    // Start a few instances with incorrect groupSize (1),
    // and Verify that Group under group id has NOT been built
    StepVerifier.create(
            Flux.merge(
                startServiceGroupDiscovery(seedAddress, groupId, 1)
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery), //
                startServiceGroupDiscovery(seedAddress, groupId, 1)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery),
                startServiceGroupDiscovery(seedAddress, groupId, 1)
                    .doOnSuccess(startedServiceDiscoveries::onNext) // track started
                    .flatMapMany(ServiceDiscovery::listenGroupDiscovery)))
        .expectSubscription()
        .expectNoEvent(TIMEOUT)
        .then(
            () -> {
              // Start shutdown process and verify again that not events will be pusblished
              startedServiceDiscoveries.flatMap(ServiceDiscovery::shutdown).then().subscribe();
            })
        .expectNoEvent(TIMEOUT)
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

  public static io.scalecube.transport.Address toAddress(Address address) {
    return io.scalecube.transport.Address.create(address.host(), address.port());
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
}
