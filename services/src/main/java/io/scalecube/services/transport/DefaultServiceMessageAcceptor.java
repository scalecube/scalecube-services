package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;

import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicBoolean;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

public final class DefaultServiceMessageAcceptor implements ServiceMessageHandler {

  private final LocalServiceHandlers serviceHandlers;

  public DefaultServiceMessageAcceptor(LocalServiceHandlers serviceHandlers) {
    this.serviceHandlers = serviceHandlers;
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    return Flux.from(toHeadAndTail(publisher)).flatMap(pair -> {
      ServiceMessage message = pair.head;
      ServiceMessageHandler dispatcher = serviceHandlers.get(message.qualifier());
      return dispatcher.invoke(Flux.from(pair.tail).startWith(message));
    });
  }

  private Publisher<HeadAndTail> toHeadAndTail(Publisher<ServiceMessage> publisher) {
    AtomicBoolean first = new AtomicBoolean(true);
    UnicastProcessor<ServiceMessage> tail = UnicastProcessor.create();
    UnicastProcessor<HeadAndTail> firstResult = UnicastProcessor.create();

    return Flux.from(publisher)
        .doOnComplete(tail::onComplete)
        .doOnError(tail::onError)
        .flatMap(message -> {
          if (first.compareAndSet(true, false)) {
            firstResult.onNext(new HeadAndTail(message, tail));
            firstResult.onComplete();
            return firstResult;
          } else {
            tail.onNext(message);
            return Flux.empty();
          }
        });
  }

  private static class HeadAndTail {
    private final ServiceMessage head;
    private final Publisher<ServiceMessage> tail;

    public HeadAndTail(ServiceMessage head, Publisher<ServiceMessage> tail) {
      this.head = head;
      this.tail = tail;
    }
  }
}
