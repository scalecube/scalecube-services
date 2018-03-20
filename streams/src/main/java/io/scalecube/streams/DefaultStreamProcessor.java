package io.scalecube.streams;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;

public final class DefaultStreamProcessor implements StreamProcessor {

  StreamMessage onErrorMessage =
      StreamMessage.qualifier(Qualifier.Q_GENERAL_FAILURE).build();

  StreamMessage onCompletedMessage =
      StreamMessage.qualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext localChannelContext;
  private final EventStream localEventStream;

  /**
   * Constructor for this stream processor.
   * 
   * @param localChannelContext local channel context
   * @param localEventStream local event stream
   */
  public DefaultStreamProcessor(ChannelContext localChannelContext, EventStream localEventStream) {
    this.localChannelContext = localChannelContext;
    this.localEventStream = localEventStream;
  }

  @Override
  public void onCompleted() {
    onNext(onCompletedMessage);
  }

  @Override
  public void onError(Throwable throwable) {
    onNext(onErrorMessage);
  }

  @Override
  public void onNext(StreamMessage message) {
    localChannelContext.postWrite(message);
  }

  @Override
  public Observable<StreamMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          localChannelContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          localChannelContext.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          localEventStream.listenChannelContextClosed()
              .subscribe(event -> onChannelContextClosed(event, emitter)));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    localChannelContext.close();
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

  private void onChannelContextClosed(Event event, Observer<StreamMessage> emitter) {
    // Hint: at this point 'event' contains ClientStream's ChannelContext (i.e. remote one) where close() was emitted
    emitter.onError(new IOException("ChannelContext closed on address: " + event.getAddress()));
  }
}
