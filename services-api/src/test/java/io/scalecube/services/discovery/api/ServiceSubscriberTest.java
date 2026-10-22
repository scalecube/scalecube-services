package io.scalecube.services.discovery.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

class ServiceSubscriberTest {

  private final Sinks.Many<ServiceDiscoveryEvent> discovery =
      Sinks.many().unicast().onBackpressureBuffer();
  private final Map<String, ServiceEndpoint> endpoints = new HashMap<>();
  private final List<String> events = new ArrayList<>();
  private final List<Throwable> errors = new ArrayList<>();

  private ServiceSubscriber<String> subscribe(boolean grouped) {
    final var builder =
        ServiceSubscriber.<String>builder()
            .filter(endpoint -> endpoint.tags().containsKey("group"))
            .onAdded(
                member -> {
                  events.add("added " + member.id());
                  return "resource of " + member.id();
                })
            .onRemoved((member, resource) -> events.add("removed " + member.id() + ", " + resource))
            .onError(errors::add);
    if (grouped) {
      builder.groupBy(endpoint -> endpoint.tags().get("group"));
    }
    final var subscriber = builder.build();
    discovery.asFlux().subscribe(subscriber);
    return subscriber;
  }

  @Test
  void testFollowsOneEndpointAndFailsOver() {
    subscribe(false);

    add("a0", "a");
    add("a1", "a");
    remove("a0");
    remove("a1");

    assertEquals(
        List.of("added a0", "removed a0, resource of a0", "added a1", "removed a1, resource of a1"),
        events);
  }

  @Test
  void testGroupIsAddedOnFirstMemberAndRemovedOnLast() {
    subscribe(true);

    add("a0", "a");
    add("a1", "a");
    remove("a0");

    assertEquals(List.of("added a0"), events);

    remove("a1");

    assertEquals(List.of("added a0", "removed a0, resource of a0"), events);
  }

  @Test
  void testFollowsOneGroupAndFailsOver() {
    subscribe(true);

    add("b0", "b");
    add("a0", "a");
    remove("b0");
    remove("a0");

    assertEquals(
        List.of("added b0", "removed b0, resource of b0", "added a0", "removed a0, resource of a0"),
        events);
  }

  @Test
  void testLeavingRemovesAndLaterRemovedIsIgnored() {
    subscribe(false);
    add("a0", "a");
    add("a1", "a");

    final var a0 = endpoints.get("a0");
    discovery.tryEmitNext(ServiceDiscoveryEvent.newEndpointLeaving(a0));
    discovery.tryEmitNext(ServiceDiscoveryEvent.newEndpointRemoved(a0));

    assertEquals(List.of("added a0", "removed a0, resource of a0", "added a1"), events);
  }

  @Test
  void testIgnoresEndpointsNotMatchingTheFilter() {
    subscribe(false);

    add("x0", null);
    remove("x0");

    assertEquals(List.of(), events);
  }

  @Test
  void testCloseRemovesFollowedOne() {
    final var subscriber = subscribe(true);
    add("a0", "a");

    subscriber.close();
    add("b0", "b");

    assertEquals(List.of("added a0", "removed a0, resource of a0"), events);
    assertTrue(subscriber.isDisposed());
  }

  @Test
  void testFailedAddGoesToOnError() {
    final var failure = new IllegalStateException("cannot add");
    final var subscriber =
        ServiceSubscriber.<String>builder()
            .onAdded(
                member -> {
                  throw failure;
                })
            .onError(errors::add)
            .build();
    discovery.asFlux().subscribe(subscriber);

    add("a0", "a");

    assertEquals(List.of(failure), errors);
  }

  @Test
  void testFailedRemoveGoesToOnError() {
    final var failure = new IllegalStateException("cannot remove");
    final var subscriber =
        ServiceSubscriber.<String>builder()
            .onAdded(member -> "resource")
            .onRemoved(
                (member, resource) -> {
                  throw failure;
                })
            .onError(errors::add)
            .build();
    discovery.asFlux().subscribe(subscriber);
    add("a0", "a");

    remove("a0");

    assertSame(failure, errors.get(0));
  }

  @Test
  void testRequiresOnAdded() {
    assertThrows(NullPointerException.class, () -> ServiceSubscriber.builder().build());
  }

  private void add(String id, String group) {
    final var tags = group != null ? Map.of("group", group) : Map.<String, String>of();
    final var endpoint =
        ServiceEndpoint.builder()
            .id(id)
            .name(id)
            .address(Address.create("localhost", 0))
            .contentTypes(Set.of())
            .serviceRegistrations(List.of())
            .tags(tags)
            .build();
    endpoints.put(id, endpoint);
    discovery.tryEmitNext(ServiceDiscoveryEvent.newEndpointAdded(endpoint));
  }

  private void remove(String id) {
    discovery.tryEmitNext(ServiceDiscoveryEvent.newEndpointRemoved(endpoints.remove(id)));
  }
}
