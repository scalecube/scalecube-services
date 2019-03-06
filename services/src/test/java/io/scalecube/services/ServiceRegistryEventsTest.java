package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.ClusterAddresses;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.api.Address;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<ServiceDiscoveryEvent> events = new ArrayList<>();

    Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .startAwait();

    seed.discovery().listen().subscribe(events::add);

    Address seedAddress = seed.discovery().address();

    Microservices ms1 =
        Microservices.builder()
            .discovery(
                (serviceRegistry, serviceEndpoint) ->
                    serviceDiscovery(serviceRegistry, serviceEndpoint, seedAddress))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new GreetingServiceImpl())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(
                (serviceRegistry, serviceEndpoint) ->
                    serviceDiscovery(serviceRegistry, serviceEndpoint, seedAddress))
            .transport(ServiceTransports::rsocketServiceTransport)
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

  private static ServiceDiscovery serviceDiscovery(
      ServiceRegistry serviceRegistry, ServiceEndpoint serviceEndpoint, Address address) {
    return new ScalecubeServiceDiscovery(serviceRegistry, serviceEndpoint)
        .options(opts -> opts.seedMembers(ClusterAddresses.toAddress(address)));
  }
}
