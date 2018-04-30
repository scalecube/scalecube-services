import io.scalecube.services.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.services.TestRequests;
import io.scalecube.services.api.ServiceMessage;

import io.scalecube.services.exceptions.BadRequestException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

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

      ServiceMessage resp = Mono.from(req).block();
      Assert.assertNotNull(resp);

  }

  @Test
  public void testCorruptedResponse() {

  }

  @Test
  public void testNotFound() {

  }

  @Test
  public void testNotAuthorized() {

  }

}
