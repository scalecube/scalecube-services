package io.scalecube.gateway.websocket;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import reactor.core.publisher.Mono;

public class WebSocketServerTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);
  public static final int COUNT = 10;

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testEcho() {
    resource.newServer(
        session -> session.send(session.receive()).then(),
        session -> Mono.empty());

    List<Echo> req = IntStream.range(0, COUNT).boxed().map(Integer::toHexString)
        .map(Echo::new).collect(Collectors.toList());

    List<Echo> responses = resource.newClientSession("/", req, Echo.class)
        .collectList()
        .block(TIMEOUT);

    Assert.assertEquals(COUNT, responses.size());
  }

  public static class Echo {
    private String text;

    public Echo() {}

    public Echo(String text) {
      this.text = text;
    }

    public String getText() {
      return text;
    }

    public void setText(String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return "Echo{" +
          "text='" + text + '\'' +
          '}';
    }
  }
}
