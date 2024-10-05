package io.scalecube.services.routings;

import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ2;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Address;
import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.routings.sut.CanaryService;
import io.scalecube.services.routings.sut.DummyRouter;
import io.scalecube.services.routings.sut.GreetingServiceImplA;
import io.scalecube.services.routings.sut.GreetingServiceImplB;
import io.scalecube.services.routings.sut.TagService;
import io.scalecube.services.routings.sut.WeightedRandomRouter;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RoutersTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static Microservices provider1;
  private static Microservices provider2;
  private static Microservices provider3;

  @BeforeAll
  public static void setup() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new));

    gatewayAddress = gateway.discoveryAddress();

    // Create microservices instance cluster.
    provider1 =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(
                    ServiceInfo.fromServiceInstance(new GreetingServiceImpl(1))
                        .tag("ONLYFOR", "joe")
                        .tag("SENDER", "1")
                        .build(),
                    ServiceInfo.fromServiceInstance(new GreetingServiceImplA())
                        .tag("Weight", "0.1")
                        .build()));

    // Create microservices instance cluster.
    provider2 =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(
                    ServiceInfo.fromServiceInstance(new GreetingServiceImpl(2))
                        .tag("ONLYFOR", "fransin")
                        .tag("SENDER", "2")
                        .build(),
                    ServiceInfo.fromServiceInstance(new GreetingServiceImplB())
                        .tag("Weight", "0.9")
                        .build()));

    TagService tagService = input -> input.map(String::toUpperCase);
    provider3 =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(
                    ServiceInfo.fromServiceInstance(tagService)
                        .tag("tagB", "bb")
                        .tag("tagC", "c")
                        .build()));
  }

  @AfterAll
  public static void tearDown() {
    gateway.close();
    provider1.close();
    provider2.close();
    provider3.close();
  }

  @Test
  public void test_router_factory() {
    assertNotNull(Routers.getRouter(RandomServiceRouter.class));

    // dummy router will always throw exception thus cannot be created.
    assertThrows(NullPointerException.class, () -> Routers.getRouter(DummyRouter.class));
  }

  @Test
  public void test_round_robin() {

    ServiceCall service = gateway.call();

    // call the service.
    GreetingResponse result1 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
            .timeout(TIMEOUT)
            .block()
            .data();
    GreetingResponse result2 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
            .timeout(TIMEOUT)
            .block()
            .data();

    assertNotEquals(result1.sender(), result2.sender());
  }

  @Test
  public void test_remote_service_tags() throws Exception {

    CanaryService service =
        gateway
            .call()
            .router(Routers.getRouter(WeightedRandomRouter.class))
            .api(CanaryService.class);

    Thread.sleep(1000);

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e3;

    Flux.range(0, n)
        .flatMap(i -> service.greeting(new GreetingRequest("joe")))
        .filter(response -> response.getResult().contains("SERVICE_B_TALKING"))
        .doOnNext(response -> serviceBCount.incrementAndGet())
        .blockLast(Duration.ofSeconds(3));

    System.out.println("Service B was called: " + serviceBCount.get() + " times out of " + n);

    assertTrue(
        (serviceBCount.doubleValue() / n) > 0.5,
        "Service B's Weight=0.9; more than half of invocations have to be routed to Service B");
  }

  @Test
  public void tesTagsFromAnnotation() {
    ServiceCall serviceCall =
        provider3
            .call()
            .router(
                (req, mes) -> {
                  ServiceReference tagServiceRef = req.listServiceReferences().get(0);
                  Map<String, String> tags = tagServiceRef.tags();
                  assertEquals(
                      new HashSet<>(asList("tagA", "tagB", "tagC", "methodTagA")), tags.keySet());
                  assertEquals("a", tags.get("tagA"));
                  // user override this tag in Microservices#services
                  assertEquals("bb", tags.get("tagB"));
                  assertEquals("c", tags.get("tagC"));
                  assertEquals("a", tags.get("methodTagA"));
                  return Optional.of(tagServiceRef);
                });
    serviceCall.api(TagService.class).upperCase(Flux.just("hello")).blockLast();
  }

  @Test
  public void test_tag_selection_logic() {

    ServiceCall service =
        gateway
            .call()
            .router(
                (reg, msg) ->
                    reg.listServiceReferences().stream()
                        .filter(ref -> "2".equals(ref.tags().get("SENDER")))
                        .findFirst());

    // call the service.
    for (int i = 0; i < 1e3; i++) {
      GreetingResponse result =
          Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
              .timeout(TIMEOUT)
              .block()
              .data();
      assertEquals("2", result.sender());
    }
  }

  @Test
  public void test_tag_request_selection_logic() {

    ServiceCall service =
        gateway
            .call()
            .router(
                (reg, msg) ->
                    reg.listServiceReferences().stream()
                        .filter(
                            ref ->
                                ((GreetingRequest) msg.data())
                                    .getName()
                                    .equals(ref.tags().get("ONLYFOR")))
                        .findFirst());

    // call the service.
    for (int i = 0; i < 1e2; i++) {
      GreetingResponse resultForFransin =
          service.requestOne(GREETING_REQUEST_REQ2, GreetingResponse.class).block(TIMEOUT).data();
      GreetingResponse resultForJoe =
          service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class).block(TIMEOUT).data();
      assertEquals("1", resultForJoe.sender());
      assertEquals("2", resultForFransin.sender());
    }
  }

  @Test
  public void test_service_tags() throws Exception {

    TimeUnit.SECONDS.sleep(3);
    ServiceCall service = gateway.call().router(WeightedRandomRouter.class);

    ServiceMessage req =
        ServiceMessage.builder()
            .qualifier(Reflect.serviceName(CanaryService.class), "greeting")
            .data(new GreetingRequest("joe"))
            .build();

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e3;
    for (int i = 0; i < n; i++) {
      ServiceMessage message = service.requestOne(req, GreetingResponse.class).block(TIMEOUT);
      if (message.data().toString().contains("SERVICE_B_TALKING")) {
        serviceBCount.incrementAndGet();
      }
    }

    System.out.println("Service B was called: " + serviceBCount.get() + " times out of " + n);

    assertTrue(
        (serviceBCount.doubleValue() / n) > 0.5,
        "Service B's Weight=0.9; at least more than half "
            + "of invocations have to be routed to Service B");
  }
}
