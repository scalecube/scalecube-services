package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterConfig.Builder;
import io.scalecube.services.a.b.testing.CanaryService;
import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.routing.Routers;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RemoteServiceTest extends BaseTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Microservices gateway;

  @Before
  public void setup() {
    this.gateway = gateway();
  }

  @After
  public void tearDown() {
    gateway.shutdown().block();
  }

  @Test
  public void test_remote_service_tags() {
    Microservices services1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImplA()).tag("Weight", "0.3").register()
        .build()
        .startAwait();

    Microservices services2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .service(new GreetingServiceImplB()).tag("Weight", "0.7").register()
        .build()
        .startAwait();

    CanaryService service = gateway.call()
        .router(Routers.getRouter(CanaryTestingRouter.class))
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

    services2.shutdown().block();
    services1.shutdown().block();
  }

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    Microservices node2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = gateway.call().create()
        .api(GreetingService.class);

    // call the service.
    Mono<GreetingResponse> result = Mono.from(service.greetingRequestTimeout(new GreetingRequest("joe", duration)));
    assertTrue(" hello to: joe".equals(result.block(Duration.ofSeconds(10)).getResult()));

    node2.shutdown().block();
  }

  @Test
  public void test_remote_void_greeting() throws Exception {
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = gateway.call().create()
        .api(GreetingService.class);

    // call the service.
    Mono.from(service.greetingVoid(new GreetingRequest("joe")))
        .block(Duration.ofSeconds(3));

    System.out.println("test_remote_void_greeting done.");

    Thread.sleep(1000);

    node1.shutdown().block();
  }

  @Test
  public void test_remote_failing_void_greeting() throws Exception {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = gateway.call().create().api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.failingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_remote_failing_void_greeting done.");

    Thread.sleep(1000);

    node1.shutdown().block();
  }

  @Test
  public void test_remote_throwing_void_greeting() throws Exception {
    // Create microservices instance.
    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    GreetingService service = gateway.call().create().api(GreetingService.class);

    GreetingRequest request = new GreetingRequest("joe");
    // call the service.
    StepVerifier.create(service.throwingVoid(request))
        .expectErrorMessage(request.toString())
        .verify(Duration.ofSeconds(3));

    System.out.println("test_remote_throwing_void_greeting done.");

    Thread.sleep(1000);

    node1.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    Mono<String> future = Mono.from(service.greeting("joe"));
    assertTrue(" hello to: joe".equals(future.block(Duration.ofSeconds(3))));
    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    Mono<String> future = Mono.from(service.greetingNoParams());

    assertTrue("hello unknown".equals(future.block(Duration.ofSeconds(1))));

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_greeting_no_params_fire_and_forget() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    service.notifyGreeting();

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(10000)).getResult()));

    provider.shutdown().block();
    consumer.shutdown().block();
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    Publisher<GreetingResponse> result =
        service.greetingRequestTimeout(new GreetingRequest("joe", Duration.ofSeconds(4)));

    Mono.from(result).doOnError(success -> {
      // print the greeting.
      System.out.println("remote_greeting_request_timeout_expires : " + success);
      assertTrue(success instanceof TimeoutException);
    });
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .seeds(provider.cluster().address())
        .build()
        .startAwait();

    // get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // call the service.
    Publisher<GreetingResponse> future = service.greetingRequest(new GreetingRequest("joe"));

    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1)).getResult()));

    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test
  public void test_remote_round_robin_selection_logic() {

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl(1))
        .build()
        .startAwait();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl(2))
        .build()
        .startAwait();

    GreetingService service = createProxy(gateway);

    GreetingResponse result1 = Mono.from(service.greetingRequest(new GreetingRequest("joe"))).block();
    GreetingResponse result2 = Mono.from(service.greetingRequest(new GreetingRequest("joe"))).block();
    assertTrue(!result1.sender().equals(result2.sender()));
    provider2.shutdown().block();
    provider1.shutdown().block();
  }

  @Test
  public void test_remote_async_greeting_return_string_service_not_found_error_case() throws Exception {
    // Create microservices instance cluster.
    Microservices provider1 = createProvider(gateway);

    GreetingService service = createProxy(gateway);
    try {
      service.greeting("hello").block(Duration.ofSeconds(3));
      fail("Expected no-reachable-member exception");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("No reachable member with such service"));
    }

    provider1.shutdown();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_using_setter() {

    CoarseGrainedServiceImpl coarseGrained = new CoarseGrainedServiceImpl();

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(greeting, coarseGrained) // add service a and b
        .build()
        .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);

    Publisher<String> future = service.callGreeting("joe");

    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1))));
  }

  @Test
  public void test_remote_serviceA_calls_serviceB() {
    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    // noinspection unused
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(greeting, another) // add service a and b
        .build()
        .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);
    Publisher<String> future = service.callGreeting("joe");
    assertTrue(" hello to: joe".equals(Mono.from(future).block(Duration.ofSeconds(1))));
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_timeout() {
    thrown.expect(InternalServiceException.class);
    thrown.expectMessage("Did not observe any item or terminal signal");

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    Microservices ms = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(greeting, another) // add service a and b
        .build()
        .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);
    Mono.from(
        service.callGreetingTimeout("joe")).block();

    System.out.println("done");
    ms.shutdown();
  }

  @Test
  public void test_remote_serviceA_calls_serviceB_with_dispatcher() throws Exception {

    // getting proxy from any node at any given time.
    CoarseGrainedServiceImpl another = new CoarseGrainedServiceImpl();

    GreetingServiceImpl greeting = new GreetingServiceImpl();

    // Create microservices instance cluster.
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(greeting, another) // add service a and b
        .build()
        .startAwait();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.call().create().api(CoarseGrainedService.class);

    String response = service.callGreetingWithDispatcher("joe").block(Duration.ofSeconds(5));
    assertEquals(response, " hello to: joe");

    provider.shutdown().block();
  }

  @Test
  public void test_services_contribute_to_cluster_metadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("HOSTNAME", "host1");
    Builder clusterConfig = ClusterConfig.builder().metadata(metadata);
    Microservices ms = Microservices.builder()
        .clusterConfig(clusterConfig)
        .services(new GreetingServiceImpl())
        .build()
        .startAwait();

    assertTrue(ms.cluster().member().metadata().containsKey("HOSTNAME"));
  }

  private GreetingService createProxy(Microservices micro) {
    return micro.call().create().api(GreetingService.class); // create proxy for GreetingService API
  }

  private Microservices createProvider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .build()
        .startAwait();
  }

  private Microservices gateway() {
    return Microservices.builder()
        .build()
        .startAwait();
  }
}
