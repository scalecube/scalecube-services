package io.scalecube.services.gateway.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;

public class CancelledSubscriber implements CoreSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(CancelledSubscriber.class);

  public static final CancelledSubscriber INSTANCE = new CancelledSubscriber();

  private CancelledSubscriber() {
    // Do not instantiate
  }

  @Override
  public void onSubscribe(org.reactivestreams.Subscription s) {
    // no-op
  }

  @Override
  public void onNext(Object o) {
    LOGGER.warn("Received ({}) which will be dropped immediately due cancelled aeron inbound", o);
  }

  @Override
  public void onError(Throwable t) {
    // no-op
  }

  @Override
  public void onComplete() {
    // no-op
  }
}
