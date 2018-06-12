package io.scalecube.services.transport;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.util.concurrent.atomic.AtomicBoolean;

public final class HeadAndTail<T> {

  private final T head;
  private final Publisher<T> tail;

  private HeadAndTail(T head, Publisher<T> tail) {
    this.head = head;
    this.tail = tail;
  }

  public static <U> Publisher<HeadAndTail<U>> createFrom(Publisher<U> publisher) {
    AtomicBoolean first = new AtomicBoolean(true);
    UnicastProcessor<U> tail = UnicastProcessor.create();
    UnicastProcessor<HeadAndTail<U>> firstResult = UnicastProcessor.create();

    return Flux.from(publisher)
        .doOnComplete(tail::onComplete)
        .doOnError(tail::onError)
        .flatMap(message -> {
          if (first.compareAndSet(true, false)) {
            firstResult.onNext(new HeadAndTail<>(message, tail));
            firstResult.onComplete();
            return firstResult;
          } else {
            tail.onNext(message);
            return Flux.empty();
          }
        });
  }

  public T head() {
    return head;
  }

  public Publisher<T> tail() {
    return tail;
  }
}
