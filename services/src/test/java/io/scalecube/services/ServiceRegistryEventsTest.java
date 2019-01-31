package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
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

    Microservices ms = new Microservices();

    Microservices seed = ms.startAwait();

    seed.discovery().listen().subscribe(events::add);

    Microservices ms1 = getMs(ms, seed);

    Microservices ms2 = getMs(ms, seed);

    Mono.when(ms1.doShutdown(), ms2.doShutdown()).block(Duration.ofSeconds(6));

    assertEquals(0, events.size());

    seed.doShutdown().block(Duration.ofSeconds(6));
  }

  private Microservices getMs(Microservices ms, Microservices seed) {
    return ms.discovery(options -> options.seeds(seed.discovery().address()))
        .services(new GreetingServiceImpl())
        .startAwait();
  }
}
