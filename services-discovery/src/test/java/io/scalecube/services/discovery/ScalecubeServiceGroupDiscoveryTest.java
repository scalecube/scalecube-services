package io.scalecube.services.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceGroupDiscovery;
import io.scalecube.services.transport.api.Address;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class ScalecubeServiceGroupDiscoveryTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  @Test
  public void testGroupIsRegistered(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize = 3;
    // Verify that Group under grop id has been built
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceGroupDiscovery::listen), //
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .thenCancel()
        .verify();
  }

  @Test
  public void testRegisterMoreThanDeclaredInTheGroup(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize_1 = 1;
    int groupSize_2 = 2;
    // Verify that several groups under same group id have been built
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress, groupId, groupSize_1)
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize_2)
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize_2)
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(
            event ->
                event.isRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .expectNextMatches(
            event ->
                event.isRegistered()
                    && (event.groupSize() == groupSize_1 || event.groupSize() == groupSize_2))
        .thenCancel()
        .verify();
  }

  @Test
  public void testSingleNodeGroupIsStillGroup(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize = 1; // group of size 1

    // Verify that group has been built and notified about that
    ReplayProcessor<ServiceGroupDiscovery> startedGroupDiscoveries = ReplayProcessor.create(1);
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress) //
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext)
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedGroupDiscoveries.flatMap(ServiceGroupDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isUnregistered() && event.groupSize() == 0)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupEventsOnBehalfOfNonGroupMember(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize = 2;
    ReplayProcessor<ServiceGroupDiscovery> startedGroupDiscoveries =
        ReplayProcessor.create(groupSize);
    // Verify that Group under grop id has been built
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext) // track started
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext) // track started
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .thenCancel()
        .verify();

    // Verify registered/unregistered group events on non-group member
    StepVerifier.create(
            startDiscoveryGroup(seedAddress) //
                .flatMapMany(ServiceGroupDiscovery::listen))
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedGroupDiscoveries.flatMap(ServiceGroupDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isUnregistered() && event.groupSize() == 0)
        .thenCancel()
        .verify();
  }

  @Test
  public void testLastOneInTheGroupStillReceiveEvents(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize = 3;
    ReplayProcessor<ServiceGroupDiscovery> startedGroupDiscoveries = ReplayProcessor.create();
    // Verify that Group under grop id has been built
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .flatMapMany(ServiceGroupDiscovery::listen), // dont track started
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext) // track started
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext) // track started
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .expectNextMatches(event -> event.isRegistered() && event.groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedGroupDiscoveries.flatMap(ServiceGroupDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(event -> event.isUnregistered() && event.groupSize() == 0)
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

  public Mono<ServiceGroupDiscovery> startDiscoveryGroup(
      Address seedAddress, String groupId, int groupSize) {
    ClusterConfig clusterConfig =
        ClusterConfig.builder().seedMembers(toAddress(seedAddress)).build();
    ServiceEndpoint serviceEndpoint = newServiceGroupEndpoint(groupId, groupSize);
    return new ScalecubeServiceGroupDiscovery(serviceEndpoint, clusterConfig).start();
  }

  private Mono<ServiceGroupDiscovery> startDiscoveryGroup(Address seedAddress) {
    ClusterConfig clusterConfig =
        ClusterConfig.builder().seedMembers(toAddress(seedAddress)).build();
    ServiceEndpoint serviceEndpoint = newServiceEndpoint();
    return new ScalecubeServiceGroupDiscovery(serviceEndpoint, clusterConfig).start();
  }
}
