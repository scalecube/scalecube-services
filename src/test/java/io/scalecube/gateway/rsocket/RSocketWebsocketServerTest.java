package io.scalecube.gateway.rsocket;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.MicroservicesExtension;
import io.scalecube.services.Microservices;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class RSocketWebsocketServerTest {

  private static final Duration VERIFY_DURATION = Duration.ofSeconds(3);

  private static final ServiceMessage GREETING_ONE = ServiceMessage.builder()
      .qualifier("/greeting/one")
      .data("hello")
      .build();

  private static final ServiceMessage GREETING_MANY_STREAM = ServiceMessage.builder()
    .qualifier("/greeting/manyStream")
    .data(5)
    .build();

  private static final ServiceMessage WRONG_QUALIFIER = ServiceMessage.builder()
      .qualifier("/zzz")
      .data("hello")
      .build();

  private static final ServiceMessage GREETING_FAILING_ONE = ServiceMessage.builder()
      .qualifier("/greeting/failing/one")
      .data("hello")
      .build();

  private static final ServiceMessage EMPTY_DATA = ServiceMessage.builder()
      .qualifier("/greeting/one")
      .build();

  @RegisterExtension
  public MicroservicesExtension microservicesExtension = new MicroservicesExtension();

  @RegisterExtension
  public RSocketWebsocketExtension rSocketWebsocketExtension = new RSocketWebsocketExtension();

  private RSocketWebsocketClient client;

  @BeforeEach
  public void setUp() {
    final Microservices gatewayMicroservice = microservicesExtension.startGateway().getGateway();

    rSocketWebsocketExtension.startGateway(gatewayMicroservice);
    microservicesExtension.startServices(gatewayMicroservice.cluster().address());
    client = rSocketWebsocketExtension.client();
  }

  @Test
  public void shouldReturnSingleResponse() {
    final Mono<ServiceMessage> result = client.requestResponse(GREETING_ONE);

    StepVerifier.create(result)
        .assertNext(serviceMessage -> assertEquals("\"Echo:hello\"", decodeData(serviceMessage.data())))
        .expectComplete()
        .verify(VERIFY_DURATION);
  }

  @Test
  public void shouldReturnManyResponses() {
    final Flux<ServiceMessage> result = client.requestStream(GREETING_MANY_STREAM);

    StepVerifier.create(result)
      .assertNext(serviceMessage -> assertEquals("0", decodeData(serviceMessage.data())))
      .expectNextCount(4)
      .expectComplete()
      .verify(VERIFY_DURATION);
  }

  @Test
  public void shouldReturnExceptionWhenQualifierIsWrong() {
    final Mono<ServiceMessage> result = client.requestResponse(WRONG_QUALIFIER);

    StepVerifier.create(result)
        .expectErrorMatches(throwable -> throwable.getMessage().startsWith("No reachable member with such service"))
        .verify(VERIFY_DURATION);
  }

  @Test
  public void shouldReturnErrorDataWhenServiceFails() {
    final Mono<ServiceMessage> result = client.requestResponse(GREETING_FAILING_ONE);

    StepVerifier.create(result)
        .assertNext(
            serviceMessage -> assertThat(decodeData(serviceMessage.data()), containsString("\"errorCode\":500")))
        .expectComplete()
        .verify(VERIFY_DURATION);
  }

  @Test
  public void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    final Mono<ServiceMessage> result = client.requestResponse(EMPTY_DATA);

    StepVerifier.create(result)
        .assertNext(
            serviceMessage -> assertThat(decodeData(serviceMessage.data()), containsString("\"errorCode\":400")))
        .expectComplete()
        .verify(VERIFY_DURATION);
  }

  private String decodeData(ByteBuf data) {
    try {
      return data.toString(StandardCharsets.UTF_8);
    } finally {
      ReferenceCountUtil.safeRelease(data);
    }
  }

}
