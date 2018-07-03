package io.scalecube.gateway.websocket;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import io.scalecube.gateway.MicroservicesResource;
import io.scalecube.gateway.WebsocketResource;
import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.core.Signal;
import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.NullData;
import io.scalecube.services.api.Qualifier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WebSocketServerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(6);

  private static final int REQUEST_NUM = 3;

  private static final long STREAM_ID = 1;

  private static final GatewayMessage GREETING_ONE =
      GatewayMessage.builder().qualifier("/greeting/one").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_FAILING_ONE =
      GatewayMessage.builder().qualifier("/greeting/failing/one").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_MANY =
      GatewayMessage.builder().qualifier("/greeting/many").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_FAILING_MANY =
      GatewayMessage.builder().qualifier("/greeting/failing/many").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_POJO_ONE =
      GatewayMessage.builder().qualifier("/greeting/pojo/one").data(new GreetingRequest("hello")).streamId(STREAM_ID)
          .build();

  private static final GatewayMessage GREETING_POJO_MANY =
      GatewayMessage.builder().qualifier("/greeting/pojo/many").data(new GreetingRequest("hello")).streamId(STREAM_ID)
          .build();

  private static final GatewayMessage GREETING_EMPTY_ONE =
      GatewayMessage.builder().qualifier("/greeting/empty/one").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_EMPTY_MANY =
      GatewayMessage.builder().qualifier("/greeting/empty/many").data("hello").streamId(STREAM_ID).build();

  @Rule
  public MicroservicesResource microservicesResource = new MicroservicesResource();

  @Rule
  public WebsocketResource websocketResource = new WebsocketResource();

  @Test
  public void testGreetingOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT, String.class));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier
          .assertNext(msg -> assertMessage("Echo:hello", msg))
          .assertNext(this::assertCompleteMessage);
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    GatewayMessage error = errorServiceMessage(500, "hello");

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_FAILING_ONE);

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT, ErrorData.class));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    int expectedResponseNum = 10;
    List<String> expected = IntStream.range(0, expectedResponseNum)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    List<String> actual =
        websocketResource.sendMessages(Mono.just(GREETING_MANY), TIMEOUT, String.class)
            .take(expectedResponseNum)
            .map(GatewayMessage::data)
            .cast(String.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testGreetingFailingMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    String content = "Echo:hello";
    GatewayMessage error = errorServiceMessage(500, content);

    StepVerifier.create(
        websocketResource.sendMessages(Mono.just(GREETING_FAILING_MANY), TIMEOUT, String.class, ErrorData.class))
        .assertNext(msg -> assertMessage(content, msg))
        .assertNext(msg -> assertMessage(content, msg))
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    microservicesResource.startGateway();
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT, ErrorData.class));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedThenStarted() {
    microservicesResource.startGateway();
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    // send many requests and expect several error responses
    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT, ErrorData.class));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);

    // start services node
    microservicesResource.startServices(microservicesResource.getGatewayAddress());

    String expectedData = "Echo:hello";

    StepVerifier
        .create(websocketResource.sendMessages(Mono.just(GREETING_ONE), TIMEOUT, String.class))
        .assertNext(msg -> assertMessage(expectedData, msg))
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    GreetingResponse expectedData = new GreetingResponse("Echo:hello");

    StepVerifier
        .create(websocketResource.sendMessages(Mono.just(GREETING_POJO_ONE), TIMEOUT, GreetingResponse.class))
        .assertNext(msg -> assertMessage(expectedData, msg))
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    int n = 10;
    List<GreetingResponse> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .collect(Collectors.toList());

    List<GreetingResponse> actual =
        websocketResource
            .sendMessages(Mono.just(GREETING_POJO_MANY), TIMEOUT, GreetingResponse.class)
            .take(n)
            .map(GatewayMessage::data)
            .cast(GreetingResponse.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidRequest() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<String> requests =
        Flux.range(0, REQUEST_NUM).map(i -> "q=/invalid/qualifier;data=invalid_message");

    GatewayMessage error = GatewayMessage.builder()
        .qualifier(Qualifier.asError(400))
        .streamId(null)
        .signal(Signal.ERROR)
        .data(new ErrorData(400, "Failed to decode message headers {headers=41, data=41}"))
        .build();

    StepVerifier.FirstStep<GatewayMessage> stepVerifier =
        StepVerifier.create(websocketResource.sendPayloads(requests, TIMEOUT, ErrorData.class));

    for (int i = 0; i < REQUEST_NUM; i++) {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    }

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingEmptyOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_EMPTY_ONE);

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT));

    IntStream.range(0, REQUEST_NUM)
        .forEach(i -> {
          stepVerifier.assertNext(msg -> assertMessage(null, msg));
          stepVerifier.assertNext(this::assertCompleteMessage);
        });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingEmptyMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_EMPTY_MANY);


    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, TIMEOUT, NullData.class));

    IntStream.range(0, REQUEST_NUM)
        .forEach(i -> {
          stepVerifier.assertNext(msg -> assertMessage(null, msg));
          stepVerifier.assertNext(this::assertCompleteMessage);
        });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  private GatewayMessage unreachableServiceMessage(String qualifier) {
    int errorCode = 503;
    String errorMessage = "No reachable member with such service: " + qualifier;
    return GatewayMessage.builder()
        .qualifier(Qualifier.asError(errorCode))
        .streamId(STREAM_ID)
        .signal(Signal.ERROR)
        .data(new ErrorData(errorCode, errorMessage))
        .build();
  }

  private GatewayMessage errorServiceMessage(int errorCode, String errorMessage) {
    return GatewayMessage.builder()
        .qualifier(Qualifier.asError(errorCode))
        .streamId(STREAM_ID)
        .signal(Signal.ERROR)
        .data(new ErrorData(errorCode, errorMessage))
        .build();
  }

  private void assertErrorMessage(GatewayMessage expected, GatewayMessage actual) {
    assertEquals(expected.qualifier(), actual.qualifier());
    assertEquals(expected.streamId(), actual.streamId());
    assertEquals(expected.signal(), actual.signal());
    assertThat(actual.data(), instanceOf(ErrorData.class));
    ErrorData expectedData = expected.data();
    ErrorData actualData = actual.data();
    assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
    assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
  }

  private void assertCompleteMessage(GatewayMessage actual) {
    assertNull(actual.qualifier());
    assertNotNull(actual.streamId());
    assertEquals(Signal.COMPLETE.code(), actual.signal().intValue());
    assertNull(actual.data());
  }

  private void assertMessage(Object expectedData, GatewayMessage actual) {
    assertNotNull(actual.qualifier());
    assertNotNull(actual.streamId());
    assertNull(actual.signal());
    assertEquals(expectedData, actual.data());
  }
}
