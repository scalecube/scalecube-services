package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.scalecube.services.api.ServiceMessage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketClientChannelTest {

  private ByteBuf data;
  private ServiceMessage request;
  private RSocketClientChannel cut;

  @BeforeEach
  void beforeEach() {
    data = Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8);
    request = ServiceMessage.builder().qualifier("greeting/one").data(data).build();
    cut =
        new RSocketClientChannel(
            Mono.<RSocket>error(new RuntimeException("connect failed")), new ServiceMessageCodec());
  }

  @Test
  void requestResponseReleasesRequestDataOnConnectFailure() {
    StepVerifier.create(cut.requestResponse(request)).expectError().verify();

    assertEquals(0, data.refCnt());
  }

  @Test
  void requestStreamReleasesRequestDataOnConnectFailure() {
    StepVerifier.create(cut.requestStream(request)).expectError().verify();

    assertEquals(0, data.refCnt());
  }
}
