package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MicroservicesTest extends BaseTest {

  @Test
  public void testStartWithoutDiscoveryWithoutTransport() {
    Map<String, String> tags = Collections.singletonMap("key", "value");
    Microservices microservices = Microservices.builder().tags(tags).startAwait();

    ServiceDiscovery serviceDiscovery = microservices.discovery();
    Assertions.assertNotNull(serviceDiscovery);
    ServiceEndpoint serviceEndpoint = serviceDiscovery.serviceEndpoint();
    Assertions.assertNotNull(serviceEndpoint);
    Assertions.assertEquals(tags, serviceEndpoint.tags());
    Assertions.assertEquals(Address.NULL_ADDRESS, serviceDiscovery.address());
    Assertions.assertEquals(Address.NULL_ADDRESS, microservices.serviceAddress());
    Assertions.assertEquals(Address.NULL_ADDRESS, serviceEndpoint.address());
  }

  @Test
  public void testStartWithoutTransport() {
    Map<String, String> tags = Collections.singletonMap("key", "value");
    Microservices microservices =
        Microservices.builder().discovery(ScalecubeServiceDiscovery::new).tags(tags).startAwait();

    ServiceDiscovery serviceDiscovery = microservices.discovery();
    Assertions.assertNotNull(serviceDiscovery);
    ServiceEndpoint serviceEndpoint = serviceDiscovery.serviceEndpoint();
    Assertions.assertNotNull(serviceEndpoint);
    Assertions.assertEquals(tags, serviceEndpoint.tags());
    Assertions.assertNotNull(serviceDiscovery.address());
    Assertions.assertEquals(Address.NULL_ADDRESS, microservices.serviceAddress());
    Assertions.assertEquals(Address.NULL_ADDRESS, serviceEndpoint.address());
  }
}
