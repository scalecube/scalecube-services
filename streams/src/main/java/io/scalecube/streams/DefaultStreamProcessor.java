package io.scalecube.streams;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action1;
import rx.internal.util.SubscriptionList;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultStreamProcessor implements StreamProcessor<StreamMessage, StreamMessage> {

  public static final StreamMessage onErrorMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_GENERAL_FAILURE).build();

  public static final StreamMessage onCompletedMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext channelContext;
  private final EventStream eventStream;

  private final AtomicBoolean isTerminated = new AtomicBoolean(); // state

  /**
   * Constructor for this stream processor.
   * 
   * @param channelContext channel context
   * @param eventStream event stream
   */
  public DefaultStreamProcessor(ChannelContext channelContext, EventStream eventStream) {
    this.channelContext = channelContext;
    this.eventStream = eventStream;
    // bind channel context to event stream
    this.eventStream.subscribe(this.channelContext);
  }

  @Override
  public void onCompleted() {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onCompletedMessage);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onErrorMessage);
    }
  }

  @Override
  public void onNext(StreamMessage message) {
    if (!isTerminated.get()) {
      channelContext.postWrite(message);
    }
  }

  @Override
  public Observable<StreamMessage> listen() {
    return Observable.create(emitter -> {

      SubscriptionList subscriptions = new SubscriptionList();
      emitter.setCancellation(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          channelContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          channelContext.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          eventStream.listenChannelContextClosed()
              .filter(event -> event.getAddress().equals(channelContext.getAddress()))
              .map(event -> new IOException("ChannelContext closed on address: " + event.getAddress()))
              .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    channelContext.close();
  }

  private void onMessage(StreamMessage message, Observer<StreamMessage> emitter) {
    String qualifier = message.qualifier();
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier)) { // remote => onCompleted
      emitter.onCompleted();
      return;
    }
    String qualifierNamespace = Qualifier.getQualifierNamespace(qualifier);
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onError
      // Hint: at this point more sophisticated exception mapping logic is needed
      emitter.onError(new RuntimeException(qualifier));
      return;
    }
    emitter.onNext(message); // remote => normal response
  }
}
