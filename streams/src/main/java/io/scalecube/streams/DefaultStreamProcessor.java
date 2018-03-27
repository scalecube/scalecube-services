package io.scalecube.streams;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultStreamProcessor implements StreamProcessor {

  public static final StreamMessage onErrorMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_ERROR).build();

  public static final StreamMessage onCompletedMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_COMPLETED).build();

  public static final StreamMessage unsubscribeMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_UNSUBSCRIBE).build();

  private final ChannelContext channelContext;
  private final EventStream eventStream;

  private final AtomicBoolean observerTerminated = new AtomicBoolean(); // state
  private final AtomicBoolean observableTerminated = new AtomicBoolean(); // state

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

    // listen remote unsubscribe
    this.channelContext.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> Qualifier.Q_UNSUBSCRIBE.asString().equalsIgnoreCase(message.qualifier()))
        .subscribe(message -> onRemoteUnsubscribe());
  }

  @Override
  public void onCompleted() {
    if (observerTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onCompletedMessage);
      tryClose();
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (observerTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onErrorMessage);
      tryClose();
    }
  }

  @Override
  public void onNext(StreamMessage message) {
    if (!observerTerminated.get()) {
      channelContext.postWrite(message);
    }
  }

  @Override
  public Observable<StreamMessage> listen() {
    return Observable.create(emitter -> {
      CompositeSubscription subscriptions = new CompositeSubscription();
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
              .filter(event -> !channelContext.getId().equals(event.getIdentity()))
              .subscribe(event -> onRemoteChannelContextClosed(event, emitter)));
    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void unsubscribe() {
    if (observableTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(unsubscribeMessage);
      tryClose();
    }
  }

  private void onMessage(StreamMessage message, Observer<StreamMessage> emitter) {
    String qualifier = message.qualifier();
    String qualifierNamespace = Qualifier.getQualifierNamespace(qualifier);
    if (Qualifier.Q_COMPLETED_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onCompleted
      emitter.onCompleted();
      return;
    }
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onError
      // Hint: at this point more sophisticated exception mapping logic is needed
      emitter.onError(new IOException(qualifier));
      return;
    }
    emitter.onNext(message); // remote => normal response
  }

  private void onRemoteChannelContextClosed(Event event, Observer<StreamMessage> emitter) {
    // Hint: at this point 'event' contains ClientStream's ChannelContext (i.e. remote one) where close() was emitted
    emitter.onError(new IOException("ChannelContext closed on address: " + event.getAddress()));
  }

  private void onRemoteUnsubscribe() {
    if (observableTerminated.compareAndSet(false, true)) {
      tryClose();
    }
  }

  private void tryClose() {
    // Hint: this alone will unsubscribe this channel context
    // from local stream => no more requests and no more replies
    channelContext.close();
  }
}
