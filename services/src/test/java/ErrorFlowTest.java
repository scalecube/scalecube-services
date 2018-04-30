import io.scalecube.services.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.services.TestRequests;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.UnauthorizedException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Mono;

import static reactor.core.publisher.Mono.*;

public class ErrorFlowTest {

  private static AtomicInteger port = new AtomicInteger(4000);
  private static Microservices provider;
  private static Microservices consumer;


  @BeforeClass
  public static void initNodes() {
    provider = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();
    consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();
  }

  @AfterClass
  public static void shutdownNodes() {
    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test(expected = BadRequestException.class)
  public void testCorruptedRequest() {
    Publisher<ServiceMessage> req = consumer
        .call().requestOne(TestRequests.GREETING_CORRUPTED_PAYLOAD_REQUEST);
    from(req).block();
    Assert.fail("Should have failed");
  }

  @Test(expected = UnauthorizedException.class)
  public void testNotAuthorized() {
    Publisher<ServiceMessage> req = consumer
            .call().requestOne(TestRequests.GREETING_UNAUTHORIZED_REQUEST);
    from(req).block();
    Assert.fail("Should have failed");
  }

  @Test(expected = BadRequestException.class)
  public void testCorruptedResponse() {
    Publisher<ServiceMessage> req = consumer
            .call().requestOne(TestRequests.GREETING_CORRUPTED_RESPONSE);
    ServiceMessage block = from(req).block();
    Assert.fail("Should have failed");
  }

  @Test
  public void testNotFound() {

  }



}
