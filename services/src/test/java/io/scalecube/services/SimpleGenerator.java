package io.scalecube.services;


import rx.Observable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleGenerator implements GeneratorService {

  private AtomicInteger sub = new AtomicInteger();

  @Override
  public Observable<Response> generate() {
    AtomicInteger i = new AtomicInteger();
    int sub = this.sub.incrementAndGet();
    return Observable.interval(1, TimeUnit.SECONDS).map(s -> new Response(sub + " => " + i.incrementAndGet()));
  }
}
