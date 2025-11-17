package io.scalecube.services;

import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

public class ServiceRegistrationTest {

  @ParameterizedTest
  @ValueSource(
      classes = {
        EchoService.class,
        GoodRestService.class,
        CreateRestService.class,
        UpdateRestService.class
      })
  void registerDuplicateService(Class<?> serviceInterface) {
    try (final var microservices =
        Microservices.start(
            new Context().services(mock(serviceInterface), mock(serviceInterface)))) {
      fail("Expected exception");
    } catch (Exception e) {
      assertInstanceOf(IllegalStateException.class, e, e::getMessage);
      assertThat(e.getMessage(), Matchers.startsWith("MethodInvoker already exists"));
    }
  }

  @Test
  void registerInvalidRestService() {
    try (final var microservices =
        Microservices.start(new Context().services(mock(BadRestService.class)))) {
      fail("Expected exception");
    } catch (Exception e) {
      assertInstanceOf(IllegalStateException.class, e, e::getMessage);
      assertThat(e.getMessage(), Matchers.startsWith("MethodInvoker already exists"));
    }
  }

  @Test
  void registerSingleValidRestService() {
    try (final var microservices =
        Microservices.start(new Context().services(mock(GoodRestService.class)))) {
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

  @Test
  void registerMultipleValidRestServices() {
    try (final var microservices =
        Microservices.start(
            new Context().services(mock(CreateRestService.class), mock(UpdateRestService.class)))) {
      final var serviceRegistry = microservices.serviceRegistry();

      final var foo = System.nanoTime();
      final var methodInvokerWithoutRestMethod =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder().qualifier("v1/service/account/" + foo).build());
      assertNull(methodInvokerWithoutRestMethod);

      final var methodInvokerByPost =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder()
                  .header(HEADER_REQUEST_METHOD, "POST")
                  .qualifier("v1/service/account/" + foo)
                  .build());
      assertNotNull(methodInvokerByPost);

      final var methodInvokerByPut =
          serviceRegistry.lookupInvoker(
              ServiceMessage.builder()
                  .header(HEADER_REQUEST_METHOD, "PUT")
                  .qualifier("v1/service/account/" + foo)
                  .build());
      assertNotNull(methodInvokerByPut);
    }
  }

  @Service("v1/service")
  interface EchoService {

    @ServiceMethod("get/:foo")
    Mono<String> echo();
  }

  @Service("v1/service")
  interface BadRestService {

    @RestMethod("GET")
    @ServiceMethod("get/:foo")
    Mono<String> echo();

    @RestMethod("GET")
    @ServiceMethod("get/:foo")
    Mono<String> ping();
  }

  @Service("v1/service")
  interface GoodRestService {

    @RestMethod("GET")
    @ServiceMethod("echo/:foo")
    Mono<String> echo();

    @RestMethod("POST")
    @ServiceMethod("echo/:foo")
    Mono<String> ping();
  }

  @Service("v1/service")
  interface CreateRestService {

    @RestMethod("POST")
    @ServiceMethod("account/:foo")
    Mono<String> account();
  }

  @Service("v1/service")
  interface UpdateRestService {

    @RestMethod("PUT")
    @ServiceMethod("account/:foo")
    Mono<String> account();
  }
}
