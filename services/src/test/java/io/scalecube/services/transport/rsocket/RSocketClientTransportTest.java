package io.scalecube.services.transport.rsocket;

import io.scalecube.services.*;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

class RSocketClientTransportTest {

  @Test
  void test_when_client_not_set_credentials() {

    int serverPort = 18810;
    String authKey = "_token";
    String authValue = "test";

    RSocketServiceTransport server = new RSocketServiceTransport()
      .serverTransportFactory(RSocketServerTransportFactory.tcp(serverPort))
      .clientTransportFactory(RSocketClientTransportFactory.tcp())
      // .credentialsSupplier((service) -> Mono.just(Collections.singletonMap(authKey, authValue)))
      // server side authenticator
      .authenticator(header -> {
        if (!Objects.equals(header.get(authKey), authValue)) {
          return Mono.error(new ForbiddenException("token invalid"));
        }
        return Mono.empty();
      });
    RSocketServiceTransport client = new RSocketServiceTransport()
      .serverTransportFactory(RSocketServerTransportFactory.tcp(18811))
      .clientTransportFactory(RSocketClientTransportFactory.tcp())
      // client not set credentials
      //.credentialsSupplier((service) -> Mono.just(Collections.singletonMap(authKey, authValue)))
      ;


    server.start().block();
    client.start().block();


    AtomicInteger callCount = new AtomicInteger();
    ServiceMethodRegistry registry = new ServiceMethodRegistryImpl();
    registry.registerService(
      ServiceInfo.fromServiceInstance((DummyService) () -> {
                   callCount.incrementAndGet();
                   return Mono.empty();
                 })
                 .errorMapper(DefaultErrorMapper.INSTANCE)
                 .dataDecoder(ServiceMessageDataDecoder.INSTANCE)
                 .build()
    );
    ServerTransport serverTransport = server
      .serverTransport(registry)
      .bind()
      .block();

    ServiceReference reference = new ServiceReference(
      new ServiceMethodDefinition("dummy"),
      new ServiceRegistration("DummyService", Collections.emptyMap(), Collections.emptyList()),
      ServiceEndpoint.builder()
                     .id(UUID.randomUUID().toString())
                     .address(serverTransport.address())
                     .build());

    ClientChannel clientChannel = client.clientTransport().create(reference);

    clientChannel
      .requestResponse(ServiceMessage
                         .builder()
                         .qualifier("/DummyService/dummy")
                         .build(),
                       Void.class)
      .as(StepVerifier::create)
      //Expect token invalid error
      .expectErrorMatches(err -> String.valueOf(err.getMessage()).contains("token invalid"))
      .verify();


  }

  @Service("DummyService")
  public interface DummyService {
    @ServiceMethod
    Mono<Void> dummy();
  }
}
