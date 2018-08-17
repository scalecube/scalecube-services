package io.scalecube.services;

import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

public final class HeadAndTail<T> {

  private final T head;
  private final Publisher<T> tail;

  private HeadAndTail(T head, Publisher<T> tail) {
    this.head = head;
    this.tail = tail;
  }

  /**
   * Create a publisher from another one, saving the head.
   *
   * @param publisher the original publisher
   * @return a new publisher that saves the head.
   */
  public static <U> Publisher<HeadAndTail<U>> createFrom(Publisher<U> publisher) {
    AtomicBoolean first = new AtomicBoolean(true);
    UnicastProcessor<U> tail = UnicastProcessor.create();
    UnicastProcessor<HeadAndTail<U>> firstResult = UnicastProcessor.create();

    return Flux.from(publisher)
        .doOnComplete(tail::onComplete)
        .doOnError(tail::onError)
        .flatMap(
            message -> {
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
