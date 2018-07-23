package io.scalecube.services;

import static io.scalecube.services.registry.api.RegistrationEvent.Type.REGISTERED;
import static io.scalecube.services.registry.api.RegistrationEvent.Type.UNREGISTERED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.registry.api.RegistrationEvent;
import io.scalecube.services.sut.GreetingServiceImpl;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import reactor.core.publisher.Mono;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<RegistrationEvent> events = new ArrayList<>();

    Microservices seed = Microservices.builder()
        .startAwait();

    seed.serviceRegistry().listen().subscribe(events::add);

    Microservices ms1 = Microservices.builder().seeds(seed.cluster().address())
        .services(new GreetingServiceImpl())
        .startAwait();

    Microservices ms2 = Microservices.builder().seeds(seed.cluster().address())
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
