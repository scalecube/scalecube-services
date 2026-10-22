package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.registry.api.ServiceRegistryPoller;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link ServiceRegistryPoller} against real nodes discovering each other: the poller of the
 * observer node's registry, polled from the test thread as an agent polls it from its duty cycle.
 */
class ServiceRegistryPollerDiscoveryTest {

  private static final String GROUP_TAG = "group";
  private static final long TIMEOUT_SECONDS = 10;

  private final List<ServiceDiscoveryEvent> discoveryEvents = new CopyOnWriteArrayList<>();
  private final List<String> added = new ArrayList<>();
  private final List<String> removed = new ArrayList<>();
  private final Set<String> mirror = new HashSet<>();
  private final List<AutoCloseable> nodes = new ArrayList<>();

  @AfterEach
  void afterEach() throws Exception {
    for (int i = nodes.size() - 1; i >= 0; i--) {
      nodes.get(i).close();
    }
  }

  @Test
  void testPollReportsJoinedMemberWithItsTags() throws Exception {
    final var observer = observerNode(null);
    final var poller = observer.serviceRegistry().poller();
    final var a0 = node(observer, "a");

    awaitPolled(poller, () -> mirror.contains(a0.id()));
    assertEquals(List.of(a0.id()), added);
    assertEquals(List.of(), removed);

    final var endpoint = endpoint(poller, a0.id());
    assertEquals("a", endpoint.tags().get(GROUP_TAG));
  }

  @Test
  void testPollReportsGracefulLeaveBeforeEndpointRemovedEvent() throws Exception {
    final var observer = observerNode(null);
    final var poller = observer.serviceRegistry().poller();
    final var a0 = node(observer, "a");
    awaitPolled(poller, () -> mirror.contains(a0.id()));

    a0.close();
    awaitPolled(poller, () -> !mirror.contains(a0.id()));
    assertEquals(List.of(a0.id()), removed);
    assertFalse(
        discoveryEvents.stream()
            .anyMatch(e -> e.isEndpointRemoved() && a0.id().equals(e.serviceEndpoint().id())),
        "removed on ENDPOINT_LEAVING, not on the later ENDPOINT_REMOVED");
    assertEquals(Set.of(), ids(poller.endpoints()));
  }

  @Test
  void testFirstPollReportsMembersRunningBeforeObserverStarted() throws Exception {
    final var seed = node(null, null);
    final var a0 = node(seed, "a");
    final var b0 = node(seed, "b");

    final var observer = observerNode(seed);
    final var poller = observer.serviceRegistry().poller();

    awaitPolled(poller, () -> mirror.containsAll(Set.of(seed.id(), a0.id(), b0.id())));
    assertEquals(Set.of(seed.id(), a0.id(), b0.id()), Set.copyOf(added));
    assertEquals(List.of(), removed);
  }

  @Test
  void testPollWithoutChangesReportsNothingAndKeepsEndpoints() throws Exception {
    final var observer = observerNode(null);
    final var poller = observer.serviceRegistry().poller();
    final var a0 = node(observer, "a");
    awaitPolled(poller, () -> mirror.contains(a0.id()));

    final var endpoints = poller.endpoints();
    assertEquals(0, poll(poller));
    assertEquals(0, poll(poller));
    assertSame(endpoints, poller.endpoints());
  }

  @Test
  void testPollerConvergesToRegistryUnderChurn() throws Exception {
    final var observer = observerNode(null);
    final var poller = observer.serviceRegistry().poller();

    final var members = new ArrayList<Microservices>();
    for (int i = 0; i < 4; i++) {
      members.add(node(observer, "g" + (i % 2)));
      poll(poller);
    }
    for (int i = 0; i < 4; i += 2) {
      members.get(i).close();
      poll(poller);
    }
    final var c0 = node(observer, "c");

    final var expected = Set.of(members.get(1).id(), members.get(3).id(), c0.id());
    awaitPolled(poller, () -> mirror.equals(expected));
    assertEquals(expected, ids(observer.serviceRegistry().listServiceEndpoints()));
    assertEquals(expected, ids(poller.endpoints()));
  }

  @Test
  void testPollersOfOneRegistryAreIndependent() throws Exception {
    final var observer = observerNode(null);
    final var first = observer.serviceRegistry().poller();
    final var a0 = node(observer, "a");
    awaitPolled(first, () -> mirror.contains(a0.id()));

    final var second = observer.serviceRegistry().poller();
    added.clear();
    assertEquals(1, poll(second));
    assertEquals(List.of(a0.id()), added);
    assertEquals(0, poll(first));
  }

  private int poll(ServiceRegistryPoller poller) {
    return poller.poll(
        endpoint -> {
          added.add(endpoint.id());
          mirror.add(endpoint.id());
        },
        endpoint -> {
          removed.add(endpoint.id());
          mirror.remove(endpoint.id());
        });
  }

  private void awaitPolled(ServiceRegistryPoller poller, BooleanSupplier condition)
      throws InterruptedException {
    added.clear();
    removed.clear();
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      poll(poller);
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    fail("not polled within " + TIMEOUT_SECONDS + "s, mirror: " + mirror);
  }

  private static ServiceEndpoint endpoint(ServiceRegistryPoller poller, String id) {
    return poller.endpoints().stream()
        .filter(endpoint -> id.equals(endpoint.id()))
        .findFirst()
        .orElseThrow();
  }

  private static Set<String> ids(Collection<ServiceEndpoint> endpoints) {
    return endpoints.stream().map(ServiceEndpoint::id).collect(Collectors.toSet());
  }

  private Microservices observerNode(Microservices seed) {
    final var context = context(seed, null);
    context.listenDiscovery().subscribe(discoveryEvents::add);
    final var node = Microservices.start(context);
    nodes.add(node);
    return node;
  }

  private Microservices node(Microservices seed, String group) {
    final var node = Microservices.start(context(seed, group).services(new GreetingServiceImpl()));
    nodes.add(node);
    return node;
  }

  private static Context context(Microservices seed, String group) {
    final var context =
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(
                            cfg ->
                                seed != null
                                    ? cfg.seedMembers(seed.discoveryAddress().toString())
                                    : cfg))
            .transport(RSocketServiceTransport::new);
    return group != null ? context.tags(Map.of(GROUP_TAG, group)) : context;
  }
}
