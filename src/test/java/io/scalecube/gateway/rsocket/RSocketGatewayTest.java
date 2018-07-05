package io.scalecube.gateway.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.MicroservicesResource;
import io.scalecube.services.Microservices;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Rule;

public class RSocketGatewayTest {

  private static final ServiceMessage GREETING_ONE =
      ServiceMessage.builder().qualifier("/greeting/one").data("hello").build();

  @Rule
  public MicroservicesResource microservicesResource = new MicroservicesResource();
  @Rule
  public RSocketGatewayResource gatewayResource = new RSocketGatewayResource();

  private RSocketGatewayClient client;

  @Before
  public void setUp() {
    final Microservices gatewayMicroservice = microservicesResource.startGateway().getGateway();

    gatewayResource.startGateway(gatewayMicroservice);
    microservicesResource.startServices(gatewayMicroservice.cluster().address());

    client = gatewayResource.client();
  }

  // TODO: implement later
  //@Test
  public void testRequestResponse() {
    final Mono<ServiceMessage> result = client.requestResponse(GREETING_ONE);

    StepVerifier.create(result)
        .assertNext(serviceMessage -> assertEquals("Echo:hello", serviceMessage.data()))
        .verifyComplete();
  }

}
