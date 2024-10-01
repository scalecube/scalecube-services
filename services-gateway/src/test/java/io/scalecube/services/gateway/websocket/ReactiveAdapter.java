package io.scalecube.services.gateway.websocket;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

public final class ReactiveAdapter extends BaseSubscriber<Object> implements ReactiveOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveAdapter.class);

  private static final AtomicLongFieldUpdater<ReactiveAdapter> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(ReactiveAdapter.class, "requested");

  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<ReactiveAdapter, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              ReactiveAdapter.class, CoreSubscriber.class, "destinationSubscriber");

  private final FluxReceive inbound = new FluxReceive();

  private volatile long requested;
  private volatile boolean fastPath;
  private long produced;
  private volatile CoreSubscriber<? super Object> destinationSubscriber;
  private Throwable lastError;

  @Override
  public boolean isDisposed() {
    return destinationSubscriber == CancelledSubscriber.INSTANCE;
  }

  @Override
  public void dispose(Throwable throwable) {
    Subscription upstream = upstream();
    if (upstream != null) {
      upstream.cancel();
    }
    CoreSubscriber<?> destination =
        DESTINATION_SUBSCRIBER.getAndSet(this, CancelledSubscriber.INSTANCE);
    if (destination != null) {
      destination.onError(throwable);
    }
  }

  @Override
  public void dispose() {
    inbound.cancel();
  }

  public Flux<Object> receive() {
    return inbound;
  }

  @Override
  public void lastError(Throwable throwable) {
    lastError = throwable;
  }

  @Override
  public Throwable lastError() {
    return lastError;
  }

  @Override
  public void tryNext(Object Object) {
    if (!isDisposed()) {
      destinationSubscriber.onNext(Object);
    } else {
      LOGGER.warn("[tryNext] reactiveAdapter is disposed, dropping : " + Object);
    }
  }

  @Override
  public boolean isFastPath() {
    return fastPath;
  }

  @Override
  public void commitProduced() {
    if (produced > 0) {
      Operators.produced(REQUESTED, this, produced);
      produced = 0;
    }
  }

  @Override
  public long incrementProduced() {
    return ++produced;
  }

  @Override
  public long requested(long limit) {
    return Math.min(requested, limit);
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    subscription.request(requested);
  }

  @Override
  protected void hookOnNext(Object Object) {
    tryNext(Object);
  }

  @Override
  protected void hookOnComplete() {
    dispose();
  }

  @Override
  protected void hookOnError(Throwable throwable) {
    dispose(throwable);
  }

  @Override
  protected void hookOnCancel() {
    dispose();
  }

  class FluxReceive extends Flux<Object> implements Subscription {

    @Override
    public void request(long n) {
      Subscription upstream = upstream();
      if (upstream != null) {
        upstream.request(n);
      }
      if (fastPath) {
        return;
      }
      if (n == Long.MAX_VALUE) {
        fastPath = true;
        requested = Long.MAX_VALUE;
        return;
      }
      Operators.addCap(REQUESTED, ReactiveAdapter.this, n);
    }

    @Override
    public void cancel() {
      Subscription upstream = upstream();
      if (upstream != null) {
        upstream.cancel();
      }
      CoreSubscriber<?> destination =
          DESTINATION_SUBSCRIBER.getAndSet(ReactiveAdapter.this, CancelledSubscriber.INSTANCE);
      if (destination != null) {
        destination.onComplete();
      }
    }

    @Override
    public void subscribe(CoreSubscriber<? super Object> destinationSubscriber) {
      boolean result =
          DESTINATION_SUBSCRIBER.compareAndSet(ReactiveAdapter.this, null, destinationSubscriber);
      if (result) {
        destinationSubscriber.onSubscribe(this);
      } else {
        Operators.error(
            destinationSubscriber,
            isDisposed()
                ? Exceptions.failWithCancel()
                : Exceptions.duplicateOnSubscribeException());
      }
    }
  }
}
