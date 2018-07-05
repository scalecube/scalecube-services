package io.scalecube.gateway.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.MicroservicesResource;
import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.services.Microservices;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketGatewayTest {

  private static final GatewayMessage GREETING_ONE =
      GatewayMessage.builder().qualifier("/greeting/one").data("hello").build();

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

  @Test
  public void testRequestResponse() {
    final Mono<GatewayMessage> result = client.requestResponse(GREETING_ONE);

    StepVerifier.create(result)
        .assertNext(gatewayMessage -> assertEquals("Echo:hello", gatewayMessage.data()))
        .verifyComplete();
  }

}
