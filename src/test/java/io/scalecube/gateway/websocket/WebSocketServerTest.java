package io.scalecube.gateway.websocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

public class WebSocketServerTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(3);
  public static final int COUNT = 10;

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testEcho() {
    resource.startServer(
        session -> session.send(session.receive()).then(),
        session -> Mono.empty());

    Flux<EchoMessage> requests = Flux.range(0, COUNT).map(String::valueOf).map(EchoMessage::new);

    List<EchoMessage> echoMessages =
        resource.sendAndReceive(requests, EchoMessage.class).collectList().block(TIMEOUT);
  }
}
