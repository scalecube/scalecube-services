package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceSubscriber;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link ServiceSubscriber} against real nodes discovering each other.
 *
 * <p>To assert that an event did not make the subscriber act, the test waits until a recorder,
 * subscribed to the same discovery events after the subscriber, has seen that event: the events
 * reach their subscribers in order, so the subscriber has handled it by then.
 */
class ServiceSubscriberDiscoveryTest {

  private static final String GROUP_TAG = "group";
  private static final long TIMEOUT_SECONDS = 10;

  private final BlockingQueue<String> events = new LinkedBlockingQueue<>();
  private final BlockingQueue<ServiceDiscoveryEvent> discoveryEvents = new LinkedBlockingQueue<>();
  private final List<AutoCloseable> nodes = new ArrayList<>();

  @AfterEach
  void afterEach() throws Exception {
    for (int i = nodes.size() - 1; i >= 0; i--) {
      nodes.get(i).close();
    }
  }

  @Test
  void testGroupIsAddedOnFirstMemberAndRemovedOnLast() throws Exception {
    final var subscriber = subscriberNode(true);
    final var a0 = member(subscriber, "a");
    final var a1 = member(subscriber, "a");

    awaitDiscovery(added(a1));
    assertEquals("added " + a0.id(), next());
    assertNoEvent();

    a0.close();
    awaitDiscovery(gone(a0));
    assertNoEvent();

    a1.close();
    assertEquals("removed " + a0.id() + " with resource-" + a0.id(), next());
  }

  @Test
  void testFailsOverToAnotherGroup() throws Exception {
    final var subscriber = subscriberNode(true);
    final var a0 = member(subscriber, "a");
    assertEquals("added " + a0.id(), next());

    final var b0 = member(subscriber, "b");
    awaitDiscovery(added(b0));
    assertNoEvent();

    a0.close();
    assertEquals("removed " + a0.id() + " with resource-" + a0.id(), next());
    assertEquals("added " + b0.id(), next());
  }

  @Test
  void testWithoutGroupByFollowsOneEndpointAndFailsOver() throws Exception {
    final var subscriber = subscriberNode(false);
    final var a0 = member(subscriber, "a");
    assertEquals("added " + a0.id(), next());

    final var a1 = member(subscriber, "a");
    awaitDiscovery(added(a1));
    assertNoEvent();

    a0.close();
    assertEquals("removed " + a0.id() + " with resource-" + a0.id(), next());
    assertEquals("added " + a1.id(), next());
  }

  @Test
  void testSeesGroupThatWasRunningBeforeSubscriberStarted() throws Exception {
    final var seed = node(null, null);
    final var a0 = node(seed, "a");

    subscriberNode(true, seed);
    assertEquals("added " + a0.id(), next());

    a0.close();
    assertEquals("removed " + a0.id() + " with resource-" + a0.id(), next());
  }

  @Test
  void testIgnoresEndpointsNotMatchingTheFilter() throws Exception {
    final var subscriber = subscriberNode(true);
    final var other = node(subscriber, null);
    awaitDiscovery(added(other));
    assertNoEvent();

    other.close();
    awaitDiscovery(gone(other));
    assertNoEvent();
  }

  @Test
  void testCloseRemovesFollowedGroup() throws Exception {
    final var serviceSubscriber = serviceSubscriber(true);
    final var subscriber = start(context(null, null), serviceSubscriber);
    final var a0 = member(subscriber, "a");
    assertEquals("added " + a0.id(), next());

    serviceSubscriber.close();
    assertEquals("removed " + a0.id() + " with resource-" + a0.id(), next());

    final var b0 = member(subscriber, "b");
    awaitDiscovery(added(b0));
    assertNoEvent();
  }

  private String next() throws InterruptedException {
    return events.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private void assertNoEvent() {
    assertNull(events.poll());
  }

  private void awaitDiscovery(Predicate<ServiceDiscoveryEvent> predicate)
      throws InterruptedException {
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      final var event = discoveryEvents.poll(100, TimeUnit.MILLISECONDS);
      if (event != null && predicate.test(event)) {
        return;
      }
    }
    fail("discovery event not seen within " + TIMEOUT_SECONDS + "s");
  }

  private static Predicate<ServiceDiscoveryEvent> added(Microservices node) {
    return event -> event.isEndpointAdded() && node.id().equals(event.serviceEndpoint().id());
  }

  private static Predicate<ServiceDiscoveryEvent> gone(Microservices node) {
    return event ->
        (event.isEndpointLeaving() || event.isEndpointRemoved())
            && node.id().equals(event.serviceEndpoint().id());
  }

  private ServiceSubscriber<String> serviceSubscriber(boolean grouped) {
    final var builder =
        ServiceSubscriber.<String>builder()
            .name("test")
            .filter(endpoint -> endpoint.tags().containsKey(GROUP_TAG))
            .onAdded(
                member -> {
                  events.add("added " + member.id());
                  return "resource-" + member.id();
                })
            .onRemoved(
                (member, resource) -> events.add("removed " + member.id() + " with " + resource))
            .onError(ex -> events.add("error " + ex));
    if (grouped) {
      builder.groupBy(endpoint -> endpoint.tags().get(GROUP_TAG));
    }
    return builder.build();
  }

  private Microservices subscriberNode(boolean grouped) {
    return subscriberNode(grouped, null);
  }

  private Microservices subscriberNode(boolean grouped, Microservices seed) {
    return start(context(seed, null), serviceSubscriber(grouped));
  }

  private Microservices start(Context context, ServiceSubscriber<String> serviceSubscriber) {
    // subscribed before start, as DefaultServiceFactory does with discovery subscriber beans;
    // the recorder after it, see the class javadoc
    context.listenDiscovery().subscribe(serviceSubscriber);
    context.listenDiscovery().subscribe(discoveryEvents::add);
    final var node = Microservices.start(context);
    nodes.add(node);
    nodes.add(serviceSubscriber);
    return node;
  }

  private Microservices member(Microservices seed, String group) {
    return node(seed, group);
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
