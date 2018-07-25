package io.scalecube.gateway.websocket;

import static io.scalecube.services.exceptions.BadRequestException.ERROR_TYPE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.gateway.MicroservicesExtension;
import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceCancelCallback;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.Signal;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WebsocketServerTest {

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

  private static final GatewayMessage GREETING_NEVER_ONE =
      GatewayMessage.builder().qualifier("/greeting/never/one").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_DELAY_ONE =
      GatewayMessage.builder().qualifier("/greeting/delay/one").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage GREETING_DELAY_MANY =
      GatewayMessage.builder().qualifier("/greeting/delay/many").data("hello").streamId(STREAM_ID).build();

  private static final GatewayMessage CANCEL_REQUEST =
      GatewayMessage.builder().streamId(STREAM_ID).signal(Signal.CANCEL).build();

  @RegisterExtension
  public MicroservicesExtension microservicesExtension = new MicroservicesExtension();

  @RegisterExtension
  public WebsocketExtension websocketExtension = new WebsocketExtension();

  @Test
  public void testGreetingOne() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(String.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier
          .assertNext(msg -> assertMessage("Echo:hello", msg))
          .assertNext(this::assertCompleteMessage);
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingOne() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage error = errorServiceMessage(STREAM_ID, 500, "hello");

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_FAILING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(GatewayMessage.from(error).streamId((long) i).build(), msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingMany() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    int expectedResponseNum = 10;
    List<String> expected = IntStream.range(0, expectedResponseNum)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    List<String> actual =
        websocketExtension
            .newInvocationForMessages(Mono.just(GREETING_MANY))
            .dataClasses(String.class)
            .invoke()
            .take(expectedResponseNum)
            .map(GatewayMessage::data)
            .cast(String.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testGreetingFailingMany() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    String content = "Echo:hello";
    GatewayMessage error = errorServiceMessage(STREAM_ID, 500, content);

    StepVerifier.create(
        websocketExtension
            .newInvocationForMessages(Mono.just(GREETING_FAILING_MANY))
            .dataClasses(String.class, ErrorData.class)
            .invoke())
        .assertNext(msg -> assertMessage(content, msg))
        .assertNext(msg -> assertMessage(content, msg))
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    microservicesExtension.startGateway();
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(GatewayMessage.from(error).streamId((long) i).build(), msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedThenStarted() {
    microservicesExtension.startGateway();
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    // send many requests and expect several error responses
    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(GatewayMessage.from(error).streamId((long) i).build(), msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);

    // start services node
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());

    String expectedData = "Echo:hello";

    StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(Mono.just(GREETING_ONE))
            .dataClasses(String.class)
            .invoke())
        .assertNext(msg -> assertMessage(expectedData, msg))
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoOne() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GreetingResponse expectedData = new GreetingResponse("Echo:hello");

    StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(Mono.just(GREETING_POJO_ONE))
            .dataClasses(GreetingResponse.class)
            .invoke())
        .assertNext(msg -> assertMessage(expectedData, msg))
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoMany() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    int n = 10;
    List<GreetingResponse> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .collect(Collectors.toList());

    List<GreetingResponse> actual =
        websocketExtension
            .newInvocationForMessages(Mono.just(GREETING_POJO_MANY))
            .dataClasses(GreetingResponse.class)
            .invoke()
            .take(n)
            .map(GatewayMessage::data)
            .cast(GreetingResponse.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidRequest() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Publisher<String> requests =
        Flux.range(0, REQUEST_NUM).map(i -> "q=/invalid/qualifier;data=invalid_message");

    GatewayMessage error = errorServiceMessage("Failed to decode message");

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForStrings(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    for (int i = 0; i < REQUEST_NUM; i++) {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    }

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingEmptyOne() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Publisher<GatewayMessage> requests = Mono.just(GatewayMessage.from(GREETING_EMPTY_ONE).streamId(STREAM_ID).build());

    StepVerifier.create(websocketExtension.newInvocationForMessages(requests).invoke())
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingEmptyMany() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_EMPTY_MANY).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .invoke());

    IntStream.range(0, REQUEST_NUM)
        .forEach(i -> stepVerifier.assertNext(this::assertCompleteMessage));

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testUnsubscribeRequest() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress(), service);
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Flux<GatewayMessage> requests = Flux.create(sink -> {
      sink.next(GREETING_MANY);
      // then send cancel request with delay
      Mono.delay(Duration.ofSeconds(TIMEOUT.getSeconds() / 2))
          .doOnSuccess($ -> {
            sink.next(CANCEL_REQUEST);
            sink.complete();
          }).subscribe();
    });

    StepVerifier.create(websocketExtension
        .newInvocationForMessages(requests)
        .dataClasses(String.class)
        .invoke())
        .thenConsumeWhile(gatewayMessage -> {
          boolean noCancelSignalYet = !gatewayMessage.hasSignal(Signal.CANCEL);
          if (noCancelSignalYet) {
            assertNotNull(gatewayMessage.data());
            assertThat(gatewayMessage.data(), startsWith("Greeting ("));
            assertThat(gatewayMessage.data(), endsWith(") to: hello"));
          }
          return noCancelSignalYet;
        })
        .assertNext(this::assertCancelMessage)
        .expectComplete()
        .verify(TIMEOUT);

    assertTrue(serviceCancelLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testUnsubscribeRequestWihtUnknownStreamId() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());
    Long unknownStreamId = -12343L;

    GatewayMessage error = errorServiceMessage(unknownStreamId, ERROR_TYPE,
        String.format("sid=%s is not contained in session", unknownStreamId));

    StepVerifier.create(websocketExtension
        .newInvocationForMessages(Mono.just(GatewayMessage.from(CANCEL_REQUEST).streamId(unknownStreamId).build()))
        .dataClasses(ErrorData.class)
        .invoke())
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testSendRequestsWithTheSameStreamId() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());
    Long streamId = 12343L;
    GatewayMessage request = GatewayMessage.from(GREETING_DELAY_ONE).streamId(streamId).build();

    GatewayMessage error = errorServiceMessage(streamId, ERROR_TYPE,
        String.format("sid=%s is already registered on session", streamId));

    Flux<GatewayMessage> requests = Flux.just(request, /* with the same streamId */request);

    StepVerifier.create(websocketExtension
        .newInvocationForMessages(requests)
        .dataClasses(String.class, ErrorData.class)
        .invoke())
        .assertNext(msg -> assertErrorMessage(error, msg))
        .assertNext(msg -> assertMessage("hello", msg))
        .assertNext(this::assertCompleteMessage)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testSendRequestsWithoutStreamId() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage error = errorServiceMessage("sid is missing");

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(null).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> stepVerifier.assertNext(msg -> assertErrorMessage(error, msg)));
    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testUnsubscribeRequestWithNonExistenceStreamId() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());
    Long nonExistenceStreamId = -12345L;

    GatewayMessage error = errorServiceMessage(nonExistenceStreamId, ERROR_TYPE,
        String.format("sid=%s is not contained in session", nonExistenceStreamId));

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GatewayMessage.from(CANCEL_REQUEST)
            .streamId(nonExistenceStreamId).build()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketExtension
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> stepVerifier
        .assertNext(msg -> assertErrorMessage(error, msg)));
    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testRequestWithInactivity() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);

    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress(), service);
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage error = errorServiceMessage(STREAM_ID, 500,
        "Did not observe any item or terminal signal within 1000ms (and no fallback has been configured)");

    GatewayMessage request = GatewayMessage.from(GREETING_DELAY_MANY).inactivity(1000).build();

    StepVerifier.create(websocketExtension
        .newInvocationForMessages(Mono.just(request))
        .dataClasses(String.class, ErrorData.class)
        .invoke())
        .thenConsumeWhile(gatewayMessage -> {
          boolean noErrorYet = !gatewayMessage.hasSignal(Signal.ERROR);
          if (noErrorYet) {
            assertNotNull(gatewayMessage.data());
            assertThat(gatewayMessage.data(), hasToString("hello"));
          }
          return noErrorYet;
        })
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);

    assertTrue(serviceCancelLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testClientShutdownConnection() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);

    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress(), service);
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage request = GatewayMessage.from(GREETING_MANY).build();

    StepVerifier.create(
        websocketExtension
            .newInvocationForMessages(Mono.just(request))
            .dataClasses(String.class)
            .sessionConsumer(session -> {
              // client shutdown its connection after 2 seconds
              Mono.delay(Duration.ofSeconds(2))
                  .doOnSuccess($ -> session.close().block(TIMEOUT))
                  .subscribe();
            })
            .invoke())
        .thenConsumeWhile(gatewayMessage -> {
          boolean noSignalYet = gatewayMessage.signal() == null;
          if (noSignalYet) {
            assertNotNull(gatewayMessage.data());
            assertThat(gatewayMessage.data(), startsWith("Greeting ("));
            assertThat(gatewayMessage.data(), endsWith(") to: hello"));
          }
          return noSignalYet;
        })
        .expectComplete()
        .verify(TIMEOUT);

    assertTrue(serviceCancelLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGatewayLostConnectionWithService() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);

    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress(), service);
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    GatewayMessage request = GatewayMessage.from(GREETING_MANY).build();

    // client shutdown its connection after 2 seconds
    Mono.delay(Duration.ofSeconds(2))
        .doOnSuccess($ -> microservicesExtension.getServices().shutdown().block())
        .subscribe();

    GatewayMessage error = errorServiceMessage(STREAM_ID, 500, "Connection closed");

    StepVerifier.create(
        websocketExtension.newInvocationForMessages(Mono.just(request))
            .dataClasses(String.class, ErrorData.class).invoke())
        .thenConsumeWhile(gatewayMessage -> {
          boolean noSignalYet = gatewayMessage.signal() == null;
          if (noSignalYet) {
            assertNotNull(gatewayMessage.data());
            assertThat(gatewayMessage.data(), startsWith("Greeting ("));
            assertThat(gatewayMessage.data(), endsWith(") to: hello"));
          }
          return noSignalYet;
        })
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);

    assertTrue(serviceCancelLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRequestWithoutQualifier() {
    microservicesExtension.startGateway();
    microservicesExtension.startServices(microservicesExtension.getGatewayAddress());
    websocketExtension.startWebsocketServer(microservicesExtension.getGateway());

    Mono<GatewayMessage> request = Mono.just(GatewayMessage.from(GREETING_ONE).qualifier(null).build());

    GatewayMessage error = errorServiceMessage(STREAM_ID, ERROR_TYPE, "q is missing");

    StepVerifier.create(websocketExtension
        .newInvocationForMessages(request)
        .dataClasses(ErrorData.class)
        .invoke())
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);
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

  private GatewayMessage errorServiceMessage(String errorMessage) {
    return errorServiceMessage(null, ERROR_TYPE, errorMessage);
  }

  private GatewayMessage errorServiceMessage(Long streamId, int errorCode, String errorMessage) {
    return GatewayMessage.builder()
        .qualifier(Qualifier.asError(errorCode))
        .streamId(streamId)
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

  private void assertCancelMessage(GatewayMessage actual) {
    assertNull(actual.qualifier());
    assertNotNull(actual.streamId());
    assertEquals(Signal.CANCEL.code(), actual.signal().intValue());
    assertNull(actual.data());
  }

  private void assertMessage(Object expectedData, GatewayMessage actual) {
    assertNotNull(actual.qualifier());
    assertNotNull(actual.streamId());
    assertNull(actual.signal());
    assertEquals(expectedData, actual.data());
  }
}
