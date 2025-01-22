package io.scalecube.services.registry;

import static io.scalecube.services.transport.jackson.JacksonCodec.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ServiceRegistryImplTest {

  private final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
  private final ServiceProviderErrorMapper errorMapper = mock(ServiceProviderErrorMapper.class);
  private final ServiceMessageDataDecoder dataDecoder = mock(ServiceMessageDataDecoder.class);

  @Test
  void testRegisterService() {
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new HelloOneImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build());
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new HelloTwoImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build());
    assertEquals(2, serviceRegistry.listServices().size());
  }

  @Test
  void testRegisterServiceRepeatedlyNotAllowed() {
    final var helloOne =
        ServiceInfo.fromServiceInstance(new HelloOneImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build();
    final var helloTwo =
        ServiceInfo.fromServiceInstance(new HelloTwoImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build();
    serviceRegistry.registerService(helloOne);
    serviceRegistry.registerService(helloTwo);
    assertEquals(2, serviceRegistry.listServices().size());
    assertThrows(IllegalStateException.class, () -> serviceRegistry.registerService(helloOne));
    assertThrows(IllegalStateException.class, () -> serviceRegistry.registerService(helloTwo));
  }

  @Test
  void testRegisterThenUnregisterServiceEndpoint() {
    final var n = 10;
    for (int i = 0; i < n; i++) {
      serviceRegistry.registerService(
          ServiceEndpoint.builder()
              .id("endpoint" + i)
              .address(Address.create("endpoint" + i, 4848))
              .contentTypes(Set.of(CONTENT_TYPE))
              .serviceRegistrations(
                  List.of(
                      new ServiceRegistration(
                          "greeting",
                          new HashMap<>(),
                          List.of(
                              new ServiceMethodDefinition("hello"),
                              new ServiceMethodDefinition("hello/:pathVar")))))
              .build());
    }

    assertEquals(n, serviceRegistry.listServiceEndpoints().size());
    assertEquals(n << 1, serviceRegistry.listServiceReferences().size());

    for (int i = 0; i < n; i++) {
      assertNotNull(serviceRegistry.unregisterService("endpoint" + i));
    }

    assertEquals(0, serviceRegistry.listServiceEndpoints().size());
    assertEquals(0, serviceRegistry.listServiceReferences().size());
  }

  @Test
  void testLookupInvoker() {
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new HelloOneImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build());
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new HelloTwoImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build());
    assertNotNull(
        serviceRegistry.lookupInvoker(
            ServiceMessage.builder().qualifier("greeting/hello").build()));
    assertNotNull(
        serviceRegistry.lookupInvoker(
            ServiceMessage.builder().qualifier("greeting/hello/12345").build()));
    assertNotNull(
        serviceRegistry.lookupInvoker(
            ServiceMessage.builder().qualifier("greeting/hello/67890").build()));
    assertNull(
        serviceRegistry.lookupInvoker(
            ServiceMessage.builder().qualifier("greeting/hola/that/not/exist").build()));
  }

  @Test
  void testLookupService() {
    final var n = 10;
    for (int i = 0; i < n; i++) {
      serviceRegistry.registerService(
          ServiceEndpoint.builder()
              .id("endpoint" + i)
              .address(Address.create("endpoint" + i, 4848))
              .contentTypes(Set.of(CONTENT_TYPE))
              .serviceRegistrations(
                  List.of(
                      new ServiceRegistration(
                          "greeting",
                          new HashMap<>(),
                          List.of(
                              new ServiceMethodDefinition("hello"),
                              new ServiceMethodDefinition("hello/:pathVar")))))
              .build());
    }
    assertEquals(
        n,
        serviceRegistry
            .lookupService(ServiceMessage.builder().qualifier("greeting/hello").build())
            .size());
    assertEquals(
        n,
        serviceRegistry
            .lookupService(ServiceMessage.builder().qualifier("greeting/hello/12345").build())
            .size());
    assertEquals(
        n,
        serviceRegistry
            .lookupService(ServiceMessage.builder().qualifier("greeting/hello/67890").build())
            .size());
    assertEquals(
        0,
        serviceRegistry
            .lookupService(
                ServiceMessage.builder().qualifier("greeting/hola/that/not/exist").build())
            .size());
  }

  @Test
  void testRegisterRestMethodsWithDifferentMethods() {
    final var restOne =
        ServiceInfo.fromServiceInstance(new RestServiceOneImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build();
    final var restTwo =
        ServiceInfo.fromServiceInstance(new RestServiceTwoImpl())
            .errorMapper(errorMapper)
            .dataDecoder(dataDecoder)
            .build();
    serviceRegistry.registerService(restOne);
    serviceRegistry.registerService(restTwo);
  }

  @Service(HelloOne.NAMESPACE)
  interface HelloOne {

    String NAMESPACE = "greeting";

    @ServiceMethod
    default Mono<String> hello() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }

  @Service(HelloTwo.NAMESPACE)
  interface HelloTwo {

    String NAMESPACE = "greeting";

    @ServiceMethod("hello/:pathVar")
    default Mono<String> helloPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }

  static class HelloOneImpl implements HelloOne {}

  static class HelloTwoImpl implements HelloTwo {}

  @Service(RestServiceOne.NAMESPACE)
  interface RestServiceOne {

    String NAMESPACE = "v1/api";

    @RestMethod("POST")
    @ServiceMethod("foo/:pathVar")
    default Mono<String> updateWithPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }

    @RestMethod("POST")
    @ServiceMethod("foo/update")
    default Mono<String> updateWithoutPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }

  @Service(RestServiceTwo.NAMESPACE)
  interface RestServiceTwo {

    String NAMESPACE = "v1/api";

    @RestMethod("PUT")
    @ServiceMethod("foo/:pathVar")
    default Mono<String> updateWithPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }

    @RestMethod("PUT")
    @ServiceMethod("foo/update")
    default Mono<String> updateWithoutPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }

  static class RestServiceOneImpl implements RestServiceOne {}

  static class RestServiceTwoImpl implements RestServiceTwo {}
}
