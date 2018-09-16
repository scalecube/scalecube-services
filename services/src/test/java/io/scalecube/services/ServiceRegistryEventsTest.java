package io.scalecube.services;

import static io.scalecube.services.registry.api.RegistryEventType.ADDED;
import static io.scalecube.services.registry.api.RegistryEventType.REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.registry.api.EndpointRegistryEvent;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<EndpointRegistryEvent> events = new ArrayList<>();

    Microservices seed = Microservices.builder().startAwait();

    seed.serviceRegistry().listenEndpointEvents().subscribe(events::add);

    Microservices ms1 =
        Microservices.builder()
            .discovery(options -> options.seeds(seed.address()))
            .services(new GreetingServiceImpl())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(options -> options.seeds(seed.address()))
            .services(new GreetingServiceImpl())
            .startAwait();

    Mono.when(ms1.shutdown(), ms2.shutdown()).block(Duration.ofSeconds(6));

    assertEquals(4, events.size());
    assertEquals(ADDED, events.get(0).type());
    assertEquals(ADDED, events.get(1).type());
    assertEquals(REMOVED, events.get(2).type());
    assertEquals(REMOVED, events.get(3).type());

    seed.shutdown().block(Duration.ofSeconds(6));
  }
}
