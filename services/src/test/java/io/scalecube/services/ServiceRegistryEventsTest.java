package io.scalecube.services;

import static io.scalecube.services.discovery.api.DiscoveryEvent.Type.REGISTERED;
import static io.scalecube.services.discovery.api.DiscoveryEvent.Type.UNREGISTERED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.api.DiscoveryEvent;
import io.scalecube.services.sut.GreetingServiceImpl;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<DiscoveryEvent> events = new ArrayList<>();

    Microservices seed = Microservices.builder()
        .startAwait();

    seed.discovery().listen().subscribe(events::add);

    Microservices ms1 = Microservices.builder().seeds(seed.discovery().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    Microservices ms2 = Microservices.builder().seeds(seed.discovery().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    Mono.when(ms1.shutdown(), ms2.shutdown()).block(Duration.ofSeconds(6));

    assertEquals(4, events.size());
    assertEquals(REGISTERED, events.get(0).type());
    assertEquals(REGISTERED, events.get(1).type());
    assertEquals(UNREGISTERED, events.get(2).type());
    assertEquals(UNREGISTERED, events.get(3).type());

    seed.shutdown().block(Duration.ofSeconds(6));
  }
}
