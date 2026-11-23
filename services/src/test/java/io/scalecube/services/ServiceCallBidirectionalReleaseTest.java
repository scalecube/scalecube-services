package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ClientTransport;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/** When no channel takes the first message of a bidirectional call, its data must be released. */
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

  private static void assertReleasedOnError(ServiceCall serviceCall) {
    final ByteBuf data = Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8);
    final ServiceMessage request =
        ServiceMessage.builder().qualifier("v1/unknown/channel").data(data).build();

    StepVerifier.create(serviceCall.requestBidirectional(Flux.just(request)))
        .expectError()
        .verify(TIMEOUT);

    assertEquals(0, data.refCnt(), "first message data refCnt");
  }
}
