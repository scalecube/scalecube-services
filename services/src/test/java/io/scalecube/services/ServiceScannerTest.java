package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;

class ServiceScannerTest {

  @Test
  void testServiceRegistrations() {
    final var serviceRegistrations =
        ServiceScanner.toServiceRegistrations(
            ServiceInfo.fromServiceInstance(new PlaceholderQualifierServiceImpl()).build());

    assertEquals(1, serviceRegistrations.size(), "serviceRegistrations.size");

    final var serviceMethodDefinitions = serviceRegistrations.get(0).methods();
    assertEquals(1, serviceMethodDefinitions.size(), "serviceMethodDefinitions.size");
    final var methodDefinition = serviceMethodDefinitions.iterator().next();

    assertEquals(
        "v1/very/cool/${microservices:id}/service/:name",
        methodDefinition.action(),
        "methodDefinition.action");
    assertEquals("GET", methodDefinition.restMethod(), "methodDefinition.restMethod");
    assertTrue(methodDefinition.isSecured(), "methodDefinition.isSecured");
    assertEquals(0, methodDefinition.tags().size(), "tags.size");
  }

  @Test
  void testReplacePlaceholders() {
    final var serviceRegistrations =
        ServiceScanner.toServiceRegistrations(
            ServiceInfo.fromServiceInstance(new PlaceholderQualifierServiceImpl()).build());

    final var microservices = mock(Microservices.class);
    final var microservicesId = "100500";
    when(microservices.id()).thenReturn(microservicesId);

    final var result = ServiceScanner.replacePlaceholders(serviceRegistrations, microservices);

    assertEquals(1, result.size(), "serviceRegistrations.size");

    final var serviceMethodDefinitions = result.get(0).methods();
    assertEquals(1, serviceMethodDefinitions.size(), "serviceMethodDefinitions.size");
    final var methodDefinition = serviceMethodDefinitions.iterator().next();

    assertEquals(
        "v1/very/cool/100500/service/:name", methodDefinition.action(), "methodDefinition.action");
    assertEquals("GET", methodDefinition.restMethod(), "methodDefinition.restMethod");
    assertTrue(methodDefinition.isSecured(), "methodDefinition.isSecured");
    assertEquals(0, methodDefinition.tags().size(), "tags.size");
  }

  @MethodSource("successMethodSource")
  @ParameterizedTest
  void testReplacePlaceholdersSuccessfully(String input, String expected) {
    // "microservices" lookup

    final var microservices = mock(Microservices.class);
    final var microservicesId = "microservicesId100500";
    when(microservices.id()).thenReturn(microservicesId);

    assertEquals(
        expected, ServiceScanner.replacePlaceholders(input, microservices), "replacePlaceholders");
  }

  @MethodSource("failedMethodSource")
  @ParameterizedTest
  void testReplacePlaceholdersFailed(String input) {
    // "microservices" lookup

    final var microservices = mock(Microservices.class);
    final var microservicesId = "microservicesId100500";
    when(microservices.id()).thenReturn(microservicesId);

    assertThrows(
        IllegalArgumentException.class,
        () -> ServiceScanner.replacePlaceholders(input, microservices));
  }

  static Stream<Arguments> successMethodSource() {
    return Stream.of(
        Arguments.of("${microservices:id}/files/:name", "microservicesId100500/files/:name"),
        Arguments.of("${microservices:id}/files/action", "microservicesId100500/files/action"));
  }

  static Stream<Arguments> failedMethodSource() {
    return Stream.of(
        Arguments.of("${}/files/:name"),
        Arguments.of("${:}/files/:name"),
        Arguments.of("${microservices}/files/:name"),
        Arguments.of("${microservices:id:kkk}/files/:name"),
        Arguments.of("${microservices:id:ggg:ggg}/files/:name"),
        Arguments.of("${microservices:}/files/:name"));
  }

  @Service
  public interface PlaceholderQualifierService {

    @RestMethod("GET")
    @ServiceMethod("v1/very/cool/${microservices:id}/service/:name")
    Mono<Void> doSomething();
  }

  @Secured
  public static class PlaceholderQualifierServiceImpl implements PlaceholderQualifierService {

    @Override
    public Mono<Void> doSomething() {
      return Mono.empty();
    }
  }
}
