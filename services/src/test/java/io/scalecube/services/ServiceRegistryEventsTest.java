package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<ServiceDiscoveryEvent> events = new ArrayList<>();

    Microservices seed = Microservices.builder().startAwait();

    seed.discovery().listen().subscribe(events::add);

    Microservices ms1 =
        Microservices.builder()
            .discovery(options -> options.seeds(seed.discovery().address()))
            .services(new GreetingServiceImpl())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(options -> options.seeds(seed.discovery().address()))
            .services(new GreetingServiceImpl())
            .startAwait();

    Mono.when(ms1.shutdown(), ms2.shutdown()).block(Duration.ofSeconds(6));

    assertEquals(4, events.size());
    assertEquals(Type.REGISTERED, events.get(0).type());
    assertEquals(Type.REGISTERED, events.get(1).type());
    assertEquals(Type.UNREGISTERED, events.get(2).type());
    assertEquals(Type.UNREGISTERED, events.get(3).type());

    seed.shutdown().block(Duration.ofSeconds(6));
  }
}
