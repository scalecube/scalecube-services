package io.scalecube.services.gateway.websocket;

import java.util.concurrent.CountDownLatch;
import reactor.core.publisher.Flux;

public class HangingServiceImpl implements HangingService {

  public final CountDownLatch subscribed = new CountDownLatch(1);

  @Override
  public Flux<String> hang(String request) {
    return Flux.<String>never().doOnSubscribe(s -> subscribed.countDown());
  }
}
