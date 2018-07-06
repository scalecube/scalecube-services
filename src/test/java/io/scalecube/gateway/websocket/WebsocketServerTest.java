package io.scalecube.gateway.websocket;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.scalecube.gateway.MicroservicesResource;
import io.scalecube.gateway.WebsocketResource;
import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.core.Signal;
import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceCancelCallback;
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

  @Rule
  public MicroservicesResource microservicesResource = new MicroservicesResource();

  @Rule
  public WebsocketResource websocketResource = new WebsocketResource();

  @Test
  public void testGreetingOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage error = errorServiceMessage(500, "hello");

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_FAILING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    int expectedResponseNum = 10;
    List<String> expected = IntStream.range(0, expectedResponseNum)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    List<String> actual =
        websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    String content = "Echo:hello";
    GatewayMessage error = errorServiceMessage(500, content);

    StepVerifier.create(
        websocketResource
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
    microservicesResource.startGateway();
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    // send many requests and expect several error responses
    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(GatewayMessage.from(error).streamId((long) i).build(), msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);

    // start services node
    microservicesResource.startServices(microservicesResource.getGatewayAddress());

    String expectedData = "Echo:hello";

    StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GreetingResponse expectedData = new GreetingResponse("Echo:hello");

    StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    int n = 10;
    List<GreetingResponse> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .collect(Collectors.toList());

    List<GreetingResponse> actual =
        websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    Publisher<String> requests =
        Flux.range(0, REQUEST_NUM).map(i -> "q=/invalid/qualifier;data=invalid_message");

    GatewayMessage error = GatewayMessage.builder()
        .qualifier(Qualifier.asError(400))
        .streamId(null)
        .signal(Signal.ERROR)
        .data(new ErrorData(400, "Failed to decode message"))
        .build();

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_EMPTY_ONE).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
            .newInvocationForMessages(requests)
            .invoke());

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
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_EMPTY_MANY).streamId(i.longValue()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
            .newInvocationForMessages(requests)
            .dataClasses(NullData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM)
        .forEach(i -> {
          stepVerifier.assertNext(msg -> assertMessage(null, msg));
          stepVerifier.assertNext(this::assertCompleteMessage);
        });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testUnsubscribeRequest() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress(), service);
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    Flux<GatewayMessage> requests = Flux.create(sink -> {
      sink.next(GREETING_MANY);
      // then send cancel request with delay
      Mono.delay(Duration.ofSeconds(TIMEOUT.getSeconds() / 2))
          .doOnSuccess($ -> {
            sink.next(CANCEL_REQUEST);
            sink.complete();
          }).subscribe();
    });

    StepVerifier.create(websocketResource
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
  public void testSendRequestsWithTheSameStreamId() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());
    Long streamId = 12343L;
    GatewayMessage request = GatewayMessage.from(GREETING_DELAY_ONE).streamId(streamId).build();

    GatewayMessage error = GatewayMessage.builder()
        .qualifier(Qualifier.asError(400))
        .streamId(streamId)
        .signal(Signal.ERROR)
        .data(new ErrorData(400, String.format("sid=%s is already registered on session", streamId)))
        .build();

    Flux<GatewayMessage> requests = Flux.just(request, /* with the same streamId */request);

    StepVerifier.create(websocketResource
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
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage error = GatewayMessage.builder()
        .qualifier(Qualifier.asError(400))
        .streamId(null)
        .signal(Signal.ERROR)
        .data(new ErrorData(400, "sid is missing"))
        .build();

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GREETING_ONE).streamId(null).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> stepVerifier.assertNext(msg -> assertErrorMessage(error, msg)));
    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testUnsubscribeRequestWithNonExistenceStreamId() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebsocketServer(microservicesResource.getGateway());
    Long nonExistenceStreamId = -12345L;

    GatewayMessage error = GatewayMessage.builder()
        .qualifier(Qualifier.asError(400))
        .streamId(nonExistenceStreamId)
        .signal(Signal.ERROR)
        .data(new ErrorData(400, String.format("sid=%s is not contained in session", nonExistenceStreamId)))
        .build();

    Publisher<GatewayMessage> requests = Flux.range(0, REQUEST_NUM)
        .map(i -> GatewayMessage.from(GatewayMessage.from(CANCEL_REQUEST)
            .streamId(nonExistenceStreamId).build()).build());

    StepVerifier.FirstStep<GatewayMessage> stepVerifier = StepVerifier
        .create(websocketResource
            .newInvocationForMessages(requests)
            .dataClasses(ErrorData.class)
            .invoke());

    IntStream.range(0, REQUEST_NUM).forEach(i -> stepVerifier.assertNext(msg -> assertErrorMessage(error, msg)));
    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testRequestWithInactivity() throws InterruptedException {
    CountDownLatch serviceCancelLatch = new CountDownLatch(1);
    GreetingService service = new GreetingServiceCancelCallback(serviceCancelLatch::countDown);

    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress(), service);
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage error = errorServiceMessage(500,
        "Did not observe any item or terminal signal within 1000ms (and no fallback has been configured)");

    GatewayMessage request = GatewayMessage.from(GREETING_DELAY_MANY).inactivity(1000).build();

    StepVerifier.create(websocketResource
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

    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress(), service);
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage request = GatewayMessage.from(GREETING_MANY).build();

    StepVerifier.create(
        websocketResource
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

    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress(), service);
    websocketResource.startWebsocketServer(microservicesResource.getGateway());

    GatewayMessage request = GatewayMessage.from(GREETING_MANY).build();

    // client shutdown its connection after 2 seconds
    Mono.delay(Duration.ofSeconds(2))
        .doOnSuccess($ -> microservicesResource.getServices().shutdown().block())
        .subscribe();

    StepVerifier.create(
        websocketResource.newInvocationForMessages(Mono.just(request)).dataClasses(String.class).invoke())
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
