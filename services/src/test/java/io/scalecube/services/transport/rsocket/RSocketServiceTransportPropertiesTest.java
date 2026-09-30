package io.scalecube.services.transport.rsocket;

import static io.scalecube.services.transport.rsocket.RSocketServiceTransport.ALLOWED_ROLES_PROP_NAME;
import static io.scalecube.services.transport.rsocket.RSocketServiceTransport.MAX_MESSAGE_SIZE_PROP_NAME;
import static io.scalecube.services.transport.rsocket.RSocketServiceTransport.MTU_PROP_NAME;
import static io.scalecube.services.transport.rsocket.RSocketServiceTransport.NUM_OF_WORKERS_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.scalecube.services.Microservices;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RSocketServiceTransportPropertiesTest {

  @Test
  void testDefaults() {
    final var transport = new RSocketServiceTransport(new Properties());
    assertEquals(Runtime.getRuntime().availableProcessors(), transport.numOfWorkers());
    assertNull(transport.allowedRoles(), "allowedRoles");
    assertEquals(0, transport.mtu(), "mtu");
    assertEquals(0, transport.maxMessageSize(), "maxMessageSize");
  }

  @Test
  void testPropertiesAreRead() {
    final var properties = new Properties();
    properties.setProperty(NUM_OF_WORKERS_PROP_NAME, "2");
    properties.setProperty(ALLOWED_ROLES_PROP_NAME, "admin, api-gateway");
    properties.setProperty(MTU_PROP_NAME, "1024");
    properties.setProperty(MAX_MESSAGE_SIZE_PROP_NAME, "4096");

    final var transport = new RSocketServiceTransport(properties);

    assertEquals(2, transport.numOfWorkers(), "numOfWorkers");
    assertEquals(Set.of("admin", "api-gateway"), transport.allowedRoles(), "allowedRoles");
    assertEquals(1024, transport.mtu(), "mtu");
    assertEquals(4096, transport.maxMessageSize(), "maxMessageSize");
  }

  @Test
  void testNullMarkerMeansNotSet() {
    final var properties = new Properties();
    properties.setProperty(NUM_OF_WORKERS_PROP_NAME, "@null");
    properties.setProperty(ALLOWED_ROLES_PROP_NAME, "@null");
    properties.setProperty(MTU_PROP_NAME, "@null");
    properties.setProperty(MAX_MESSAGE_SIZE_PROP_NAME, "@null");

    final var transport = new RSocketServiceTransport(properties);

    assertEquals(Runtime.getRuntime().availableProcessors(), transport.numOfWorkers());
    assertNull(transport.allowedRoles(), "allowedRoles");
    assertEquals(0, transport.mtu(), "mtu");
    assertEquals(0, transport.maxMessageSize(), "maxMessageSize");
  }

  @Test
  void testInvalidMtuPropertyIsRejected() {
    final var properties = new Properties();
    properties.setProperty(MTU_PROP_NAME, "10");
    assertThrows(IllegalArgumentException.class, () -> new RSocketServiceTransport(properties));
  }

  @Test
  void testSettersMutateInPlace() {
    final var transport = new RSocketServiceTransport(new Properties());
    assertSame(transport, transport.numOfWorkers(3).mtu(1024));
    assertEquals(3, transport.numOfWorkers());
    assertEquals(1024, transport.mtu());
  }

  @Test
  void testMicroservicesContextProperties() {
    final var properties = new Properties();
    properties.setProperty(Microservices.Context.NAME_PROP_NAME, "svc");
    properties.setProperty(Microservices.Context.EXTERNAL_HOST_PROP_NAME, "ext-host");
    properties.setProperty(Microservices.Context.EXTERNAL_PORT_PROP_NAME, "7070");

    final var context = new Microservices.Context(properties);

    assertSame(properties, context.properties());
    assertEquals("svc", context.name(), "name");
    assertEquals("ext-host", context.externalHost(), "externalHost");
    assertEquals(7070, context.externalPort(), "externalPort");
  }
}
