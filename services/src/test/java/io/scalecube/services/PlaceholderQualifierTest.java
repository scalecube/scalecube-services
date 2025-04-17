package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

public class PlaceholderQualifierTest {

  private static Microservices gateway;
  private static Microservices providerFoo1;
  private static Microservices providerFoo2;

  @BeforeAll
  public static void setup() {
    Hooks.onOperatorDebug();

    gateway = gateway();

    providerFoo1 =
        newServiceProvider(
            List.of(gateway.discoveryAddress().toString()),
            ServiceInfo.fromServiceInstance(new FooServiceImpl()).build());

    providerFoo2 =
        newServiceProvider(
            List.of(gateway.discoveryAddress().toString()),
            ServiceInfo.fromServiceInstance(new FooServiceImpl()).build());
  }

  @AfterAll
  public static void tearDown() {
    try {
      gateway.close();
    } catch (Exception ignore) {
      // no-op
    }

    try {
      providerFoo1.close();
    } catch (Exception ignore) {
      // no-op
    }

    try {
      providerFoo2.close();
    } catch (Exception ignore) {
      // no-op
    }
  }

  @Test
  void shouldRouteByPlaceholderQualifier() {
    final var foo1Id = providerFoo1.id();
    final String foo1Result =
        gateway
            .call()
            .requestOne(
                ServiceMessage.builder().qualifier("v1/api/hello/" + foo1Id).build(), String.class)
            .block()
            .data();
    assertEquals(foo1Id, foo1Result);

    final var foo2Id = providerFoo1.id();
    final String foo2Result =
        gateway
            .call()
            .requestOne(
                ServiceMessage.builder().qualifier("v1/api/hello/" + foo2Id).build(), String.class)
            .block()
            .data();
    assertEquals(foo2Id, foo2Result);
  }

  @Test
  void shouldRouteByPlaceholderQualifierWithPathVar() {
    final var foo1Id = providerFoo1.id();
    final var name1 = "name1";
    final String foo1Result =
        gateway
            .call()
            .requestOne(
                ServiceMessage.builder().qualifier("v1/api/hello/" + foo1Id + "/" + name1).build(),
                String.class)
            .block()
            .data();
    assertEquals(foo1Id + "|" + name1, foo1Result);

    final var foo2Id = providerFoo1.id();
    final var name2 = "name2";
    final String foo2Result =
        gateway
            .call()
            .requestOne(
                ServiceMessage.builder().qualifier("v1/api/hello/" + foo2Id + "/" + name2).build(),
                String.class)
            .block()
            .data();
    assertEquals(foo2Id + "|" + name2, foo2Result);
  }

  private static Microservices gateway() {
    return Microservices.start(
        new Context()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, null))
            .transport(RSocketServiceTransport::new));
  }

  private static Microservices newServiceProvider(
      List<String> seedMembers, ServiceInfo serviceInfo) {
    return Microservices.start(
        new Context()
            .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, seedMembers))
            .transport(RSocketServiceTransport::new)
            .services(serviceInfo));
  }

  private static ServiceDiscovery serviceDiscovery(
      ServiceEndpoint serviceEndpoint, List<String> seedMembers) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(serviceEndpoint))
        .membership(
            cfg -> {
              if (seedMembers != null) {
                return cfg.seedMembers(seedMembers);
              } else {
                return cfg;
              }
            });
  }

  @Service("v1/api")
  public interface FooService {

    @ServiceMethod("hello/${microservices:id}")
    Mono<String> hello();

    @ServiceMethod("hello/${microservices:id}/:name")
    Mono<String> helloWithPathVar();
  }

  public static class FooServiceImpl implements FooService {

    private String id;

    @AfterConstruct
    void conclude(Microservices microservices) {
      id = microservices.id();
    }

    @Override
    public Mono<String> hello() {
      return Mono.just(id);
    }

    @Override
    public Mono<String> helloWithPathVar() {
      return RequestContext.deferContextual().map(context -> id + "|" + context.pathVar("name"));
    }
  }
}
