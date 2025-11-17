package io.scalecube.services.gateway.rest;

import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class RestServiceDefinitionTest {

  @Test
  void registerInvalidService() {
    try {
      Microservices.start(new Context().services(mock(BadService.class)));
      fail("Expected exception");
    } catch (Exception e) {
      assertInstanceOf(IllegalArgumentException.class, e, e::getMessage);
      assertThat(e.getMessage(), Matchers.startsWith("Duplicate method found for method"));
    }
  }

  @Test
  void registerValidService() {
    try (final var microservices =
        Microservices.start(new Context().services(mock(GoodService.class)))) {
      final var serviceRegistry = microservices.serviceRegistry();

      final var foo = System.nanoTime();
      final var methodInvokerWithoutRestMethod =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder().qualifier("v1/service/echo/" + foo).build());
      assertNull(methodInvokerWithoutRestMethod);

      final var methodInvokerByGet =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder()
                  .header(HEADER_REQUEST_METHOD, "GET")
                  .qualifier("v1/service/echo/" + foo)
                  .build());
      assertNotNull(methodInvokerByGet);

      final var methodInvokerByPost =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder()
                  .header(HEADER_REQUEST_METHOD, "POST")
                  .qualifier("v1/service/echo/" + foo)
                  .build());
      assertNotNull(methodInvokerByPost);
    }
  }

  @Service("v1/service")
  interface BadService {

    @RestMethod("GET")
    @ServiceMethod("get/:foo")
    Mono<SomeResponse> echo();

    @RestMethod("GET")
    @ServiceMethod("get/:foo")
    Mono<SomeResponse> ping();
  }

  @Service("v1/service")
  interface GoodService {

    @RestMethod("GET")
    @ServiceMethod("echo/:foo")
    Mono<SomeResponse> echo();

    @RestMethod("POST")
    @ServiceMethod("echo/:foo")
    Mono<SomeResponse> ping();
  }
}
