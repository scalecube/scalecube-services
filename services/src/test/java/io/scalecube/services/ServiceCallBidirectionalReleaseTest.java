package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientChannel;
import io.scalecube.services.transport.rsocket.ServiceMessageCodec;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * The first message of a bidirectional call is owned by the channel once it subscribes to the
 * messages; if it never does, its data must be released by the call.
 */
class ServiceCallBidirectionalReleaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Test
  void noRouteReleasesFirstMessage() {
    final ServiceCall serviceCall =
        new ServiceCall()
            .transport(mock(ClientTransport.class))
            .router((registry, request) -> Optional.empty());

    assertReleasedOnError(serviceCall);
  }

  @Test
  void noTransportReleasesFirstMessage() {
    final ServiceCall serviceCall =
        new ServiceCall().router((registry, request) -> Optional.empty());

    assertReleasedOnError(serviceCall);
  }

  @Test
  void failingTransportCreateReleasesFirstMessage() {
    final ClientTransport transport = mock(ClientTransport.class);
    when(transport.create(any())).thenThrow(new IllegalStateException("create failed"));
    final ServiceCall serviceCall =
        new ServiceCall()
            .transport(transport)
            .router((registry, request) -> Optional.of(mock(ServiceReference.class)));

    assertReleasedOnError(serviceCall);
  }

  @Test
  void connectFailureReleasesFirstMessage() {
    final ClientChannel channel =
        new RSocketClientChannel(
            Mono.error(new ConnectException("Connection refused")), new ServiceMessageCodec());

    assertReleasedOnError(serviceCall(channel));
  }

  @Test
  void channelFailingWithoutSubscribingReleasesFirstMessage() {
    // as the gateway client transports do: requestChannel is not supported
    final ClientChannel channel = mock(ClientChannel.class);
    when(channel.requestChannel(any()))
        .thenReturn(Flux.error(new UnsupportedOperationException("requestChannel")));

    assertReleasedOnError(serviceCall(channel));
  }

  @Test
  void cancelBeforeChannelSubscribesReleasesFirstMessage() {
    // channel still connecting: it has not subscribed to the messages yet
    final ClientChannel channel = mock(ClientChannel.class);
    when(channel.requestChannel(any())).thenReturn(Flux.never());
    final ByteBuf data = newData();

    final var disposable =
        serviceCall(channel).requestBidirectional(Flux.just(newRequest(data))).subscribe();
    verify(channel).requestChannel(any()); // the call reached the channel
    disposable.dispose();

    assertEquals(0, data.refCnt(), "first message data refCnt");
  }

  @Test
  void channelTakingFirstMessageOwnsIt() {
    final ClientChannel channel = mock(ClientChannel.class);
    when(channel.requestChannel(any()))
        .thenAnswer(
            invocation ->
                Flux.from(invocation.<Publisher<ServiceMessage>>getArgument(0))
                    .doOnNext(message -> ((ByteBuf) message.data()).release())
                    .thenMany(Flux.empty()));
    final ByteBuf data = newData().retain(); // one reference for the channel, one for the test

    StepVerifier.create(serviceCall(channel).requestBidirectional(Flux.just(newRequest(data))))
        .verifyComplete();

    assertEquals(1, data.refCnt(), "released by the channel only, not again by ServiceCall");
  }

  private static ServiceCall serviceCall(ClientChannel channel) {
    final ClientTransport transport = mock(ClientTransport.class);
    when(transport.create(any())).thenReturn(channel);
    return new ServiceCall()
        .transport(transport)
        .router((registry, request) -> Optional.of(mock(ServiceReference.class)));
  }

  private static ByteBuf newData() {
    return Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8);
  }

  private static ServiceMessage newRequest(ByteBuf data) {
    return ServiceMessage.builder().qualifier("v1/unknown/channel").data(data).build();
  }

  private static void assertReleasedOnError(ServiceCall serviceCall) {
    final ByteBuf data = newData();

    StepVerifier.create(serviceCall.requestBidirectional(Flux.just(newRequest(data))))
        .expectError()
        .verify(TIMEOUT);

    assertEquals(0, data.refCnt(), "first message data refCnt");
  }
}
