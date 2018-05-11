package io.scalecube.gateway.websocket;

import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class WebSocketServerTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testEcho() {
    System.out.println(
        resource.newServer(
            session -> {
              return session.receive().log().then();
            },
            session -> {
              System.out.println("Thank you, good bye");
              return Mono.empty();
            }));

    Flux<Object> req = Flux.range(1, 1).map(Integer::toHexString).map(Echo::new);
    Flux<Object> resp = resource.newClientSession("/", req);

    Object blockLast = resp.blockLast(TIMEOUT);
    System.out.println(blockLast);
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
