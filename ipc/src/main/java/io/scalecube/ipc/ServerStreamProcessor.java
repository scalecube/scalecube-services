package io.scalecube.ipc;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;

public final class ServerStreamProcessor implements StreamProcessor {

  private final EventStream localStream;
  private final ChannelContext localContext;

  /**
   * Constructor for this stream processor.
   * 
   * @param localContext local channel context
   * @param localStream local event stream
   */
  public ServerStreamProcessor(ChannelContext localContext, EventStream localStream) {
    this.localContext = localContext;
    this.localStream = localStream;
  }

  @Override
  public void onCompleted() {
    onNext(onCompletedMessage);
  }

  @Override
  public void onError(Throwable e) {
    onNext(onErrorMessage);
  }

  @Override
  public void onNext(ServiceMessage message) {
    localContext.postWrite(message);
  }

  @Override
  public Observable<ServiceMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          localContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          localContext.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          localStream.listenChannelContextClosed()
              .subscribe(event -> onChannelContextClosed(event, emitter)));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    localContext.close();
  }

  private void onMessage(ServiceMessage message, Observer<ServiceMessage> emitter) {
    String qualifier = message.getQualifier();
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

  private void onChannelContextClosed(Event event, Observer<ServiceMessage> emitter) {
    // Hint: at this point 'event' contains ClientStream's ChannelContext (i.e. remote one) where close() was emitted
    emitter.onError(new IOException("ChannelContext closed on address: " + event.getAddress()));
  }
}
