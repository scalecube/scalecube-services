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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RoutersTest extends BaseTest {
  public static final int TIMEOUT = 10;
  private Duration timeout = Duration.ofSeconds(TIMEOUT);

  private static Microservices gateway;
  private static Microservices provider1;
  private static Microservices provider2;

  @BeforeAll
  public static void setup() {
    gateway = Microservices.builder().startAwait();
 // Create microservices instance cluster.
    provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImpl(1)).tag("ONLYFOR", "joe").tag("SENDER", "1").register()
        .service(new GreetingServiceImplA()).tag("Weight", "0.3").register()
        .startAwait();

    // Create microservices instance cluster.
    provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImpl(2)).tag("ONLYFOR", "fransin").tag("SENDER", "2").register()
        .service(new GreetingServiceImplB()).tag("Weight", "0.7").register()
        .startAwait();
    
  }

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
    assertThrows(IllegalArgumentException.class, () -> Routers.getRouter(DummyRouter.class));
  }

  @Test
  public void test__round_robin() {
    
    ServiceCall service = gateway.call().create();

    // call the service.
    GreetingResponse result1 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block()
            .data();
    GreetingResponse result2 =
        Mono.from(service.requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block()
            .data();

    assertTrue(!result1.sender().equals(result2.sender()));
  }
    
  @Test
  public void test_remote_service_tags() {

    CanaryService service = gateway.call()
        .router(Routers.getRouter(WeightedRandomRouter.class))
        .create()
        .api(CanaryService.class);

    Util.sleep(1000);

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e2;
    for (int i = 0; i < n; i++) {
      GreetingResponse success = service.greeting(new GreetingRequest("joe")).block(Duration.ofSeconds(3));
      if (success.getResult().contains("SERVICE_B_TALKING")) {
        serviceBCount.incrementAndGet();
      }
    }

    assertEquals(0.6d, serviceBCount.doubleValue() / n, 0.2d);

  }
  
  @Test
  public void test_tag_selection_logic() {
    
    Call service = gateway.call().router((reg, msg) -> reg.listServiceReferences().stream().filter(ref -> "2".equals(
        ref.tags().get("SENDER"))).findFirst());

    // call the service.
    for (int i = 0; i < 1e3; i++) {
      GreetingResponse result =
          Mono.from(service.create().requestOne(GREETING_REQUEST_REQ, GreetingResponse.class)).timeout(timeout).block()
              .data();
      assertEquals("2", result.sender());
    }
    
  }

  @Test
  public void test_tag_request_selection_logic() {


    ServiceCall service = gateway.call().router(
        (reg, msg) -> reg.listServiceReferences().stream().filter(ref -> ((GreetingRequest) msg.data()).getName()
            .equals(ref.tags().get("ONLYFOR"))).findFirst())
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

    ServiceMessage req = ServiceMessage.builder()
        .qualifier(Reflect.serviceName(CanaryService.class), "greeting")
        .data(new GreetingRequest("joe"))
        .build();

    AtomicInteger serviceBCount = new AtomicInteger(0);

    int n = (int) 1e2;
    for (int i = 0; i < n; i++) {
      ServiceMessage message = service.requestOne(req, GreetingResponse.class).block(timeout);
      if (message.data().toString().contains("SERVICE_B_TALKING")) {
        serviceBCount.incrementAndGet();
      }
    }

    System.out.println("Service B was called: " + serviceBCount.get() + " times.");

    assertEquals(0.6d, serviceBCount.doubleValue() / n, 0.25d);

  }

}
