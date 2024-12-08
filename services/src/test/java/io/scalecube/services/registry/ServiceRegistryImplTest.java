package io.scalecube.services.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceRegistration;
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

  private final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl(null);
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
              .contentTypes(Set.of("json"))
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
  void testGetInvoker() {
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
    assertNotNull(serviceRegistry.getInvoker("greeting/hello"));
    assertNotNull(serviceRegistry.getInvoker("greeting/hello/12345"));
    assertNotNull(serviceRegistry.getInvoker("greeting/hello/67890"));
    assertNull(serviceRegistry.getInvoker("greeting/hola/that/not/exist"));
  }

  @Test
  void testLookupService() {
    final var n = 10;
    for (int i = 0; i < n; i++) {
      serviceRegistry.registerService(
          ServiceEndpoint.builder()
              .id("endpoint" + i)
              .address(Address.create("endpoint" + i, 4848))
              .contentTypes(Set.of("application/json"))
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

  @Service(HelloOne.NAMESPACE)
  interface HelloOne {

    String NAMESPACE = "greeting";

    @ServiceMethod
    Mono<String> hello();
  }

  @Service(HelloTwo.NAMESPACE)
  interface HelloTwo {

    String NAMESPACE = "greeting";

    @ServiceMethod("hello/:pathVar")
    Mono<String> helloPathVar();
  }

  static class HelloOneImpl implements HelloOne {

    @Override
    public Mono<String> hello() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }

  static class HelloTwoImpl implements HelloTwo {

    @Override
    public Mono<String> helloPathVar() {
      return Mono.just("" + System.currentTimeMillis());
    }
  }
}
