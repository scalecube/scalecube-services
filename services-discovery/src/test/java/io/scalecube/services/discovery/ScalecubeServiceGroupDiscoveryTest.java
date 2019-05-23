package io.scalecube.services.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceGroupDiscovery;
import io.scalecube.services.transport.api.Address;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  public void testGroupEventOnBehalfOfNonGroupMember(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();

    int groupSize = 2;
    ReplayProcessor<ServiceGroupDiscovery> startedGroupDiscoveries = ReplayProcessor.create();
    // Verify that Group under grop id has been built
    StepVerifier.create(
            Flux.merge(
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext)
                    .flatMapMany(ServiceGroupDiscovery::listen),
                startDiscoveryGroup(seedAddress, groupId, groupSize)
                    .doOnSuccess(startedGroupDiscoveries::onNext)
                    .flatMapMany(ServiceGroupDiscovery::listen)))
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .thenCancel()
        .verify();

    // Verify registered group events on non-group member
    StepVerifier.create(
            startDiscoveryGroup(seedAddress) //
                .flatMapMany(ServiceGroupDiscovery::listen))
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .then(
            () -> {
              // Start shutdown process
              startedGroupDiscoveries.flatMap(ServiceGroupDiscovery::shutdown).then().subscribe();
            })
        .expectNextMatches(
            event ->
                event.isUnregistered()
                    && !event.endpointGroup().serviceEndpoints().isPresent()
                    && event.endpointGroup().groupSize() == 0)
        .thenCancel()
        .verify();
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
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .expectNextMatches(
            event -> event.isRegistered() && event.endpointGroup().groupSize() == groupSize)
        .thenCancel()
        .verify();
  }

  @Test
  public void testGroupIsUnregistered(TestInfo testInfo) {
    String groupId = Integer.toHexString(testInfo.getDisplayName().hashCode());

    ServiceDiscovery seed = new ScalecubeServiceDiscovery(newServiceEndpoint()).start().block();

    Address seedAddress = seed.address();
  }

  public static ServiceEndpoint newServiceEndpoint() {
    return new ServiceEndpoint(
        UUID.randomUUID().toString(),
        null,
        Collections.emptySet(),
        Collections.emptyMap(),
        Collections.emptyList());
  }

  public static ServiceEndpoint newServiceGroupEndpoint(String groupId, int groupSize) {
    Map<String, String> tags = new HashMap<>();
    tags.put(ServiceGroup.GROUP_ID, groupId);
    tags.put(ServiceGroup.GROUP_SIZE, "" + groupSize);
    return new ServiceEndpoint(
        UUID.randomUUID().toString(), null, Collections.emptySet(), tags, Collections.emptyList());
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
