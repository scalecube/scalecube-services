package io.scalecube.services.routings;

import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ;
import static io.scalecube.services.TestRequests.GREETING_REQUEST_REQ2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.routings.sut.CanaryService;
import io.scalecube.services.routings.sut.DummyRouter;
import io.scalecube.services.routings.sut.GreetingServiceImplA;
import io.scalecube.services.routings.sut.GreetingServiceImplB;
import io.scalecube.services.routings.sut.WeightedRandomRouter;
import io.scalecube.services.sut.GreetingRequest;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RoutersTest extends BaseTest {
  public static final int TIMEOUT = 10;
  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private static Microservices gateway;
  private static Microservices provider1;
  private static Microservices provider2;

  /** Setup. */
  @BeforeAll
  public static void setup() {
    gateway = Microservices.builder().startAwait();
    // Create microservices instance cluster.
    provider1 =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImpl(1))
                    .tag("ONLYFOR", "joe")
                    .tag("SENDER", "1")
                    .build(),
                ServiceInfo.fromServiceInstance(new GreetingServiceImplA())
                    .tag("Weight", "0.1")
                    .build())
            .startAwait();

    // Create microservices instance cluster.
    provider2 =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImpl(2))
                    .tag("ONLYFOR", "fransin")
                    .tag("SENDER", "2")
                    .build(),
                ServiceInfo.fromServiceInstance(new GreetingServiceImplB())
                    .tag("Weight", "0.9")
                    .build())
            .startAwait();
  }

  /** Cleanup. */
  @AfterAll
  public static void tearDown() {
    gateway.shutdown();
    provider1.shutdown();
    provider2.shutdown();
  }

  @Test
  public void test_router_factory() {
    assertNotNull(Routers.getRouter(RandomServiceRouter.class));

    // dummy router will always throw exception thus cannot be created.
    assertThrows(NullPointerException.class, () -> Routers.getRouter(DummyRouter.class));
  }

  @Test
  public void test_round_robin() {

    ServiceCall service = gateway.call().create();

    // call the service.
    GreetingResponse result1 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
            .timeout(timeout)
            .block()
            .data();
    GreetingResponse result2 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
            .timeout(timeout)
            .block()
            .data();

    assertTrue(!result1.sender().equals(result2.sender()));
  }

  @Test
  public void test_remote_service_tags() throws Exception {

    CanaryService service =
        gateway
            .call()
            .router(Routers.getRouter(WeightedRandomRouter.class))
            .create()
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
  public void test_tag_selection_logic() {

    Call service =
        gateway
            .call()
            .router(
                (reg, msg) ->
                    reg.listServiceReferences()
                        .stream()
                        .filter(ref -> "2".equals(ref.tags().get("SENDER")))
                        .findFirst());

    // call the service.
    for (int i = 0; i < 1e3; i++) {
      GreetingResponse result =
          Mono.from(service.create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class))
              .timeout(timeout)
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
                    reg.listServiceReferences()
                        .stream()
                        .filter(
                            ref ->
                                ((GreetingRequest) msg.data())
                                    .getName()
                                    .equals(ref.tags().get("ONLYFOR")))
                        .findFirst())
            .create();

    // call the service.
    for (int i = 0; i < 1e2; i++) {
      GreetingResponse resultForFransin =
          service.requestOne(GREETING_REQUEST_REQ2, GreetingResponse.class).block(timeout).data();
      GreetingResponse resultForJoe =
          service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class).block(timeout).data();
      assertEquals("1", resultForJoe.sender());
      assertEquals("2", resultForFransin.sender());
    }
  }

  @Test
  public void test_service_tags() throws Exception {

    TimeUnit.SECONDS.sleep(3);
    ServiceCall service = gateway.call().router(WeightedRandomRouter.class).create();

    ServiceMessage req =
        ServiceMessage.builder()
            .qualifier(Reflect.serviceName(CanaryService.class), "greeting")
            .data(new GreetingRequest("joe"))
            .build();

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e3;
    for (int i = 0; i < n; i++) {
      ServiceMessage message = service.requestOne(req, GreetingResponse.class).block(timeout);
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
