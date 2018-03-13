package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;

public final class ClientStreamProcessor implements StreamProcessor {

  private final ServerStream localStream;
  private final ChannelContext localContext;

  /**
   * @param address of the target endpoing of where to send request traffic.
   * @param localStream shared serverStream (among {@link ClientStreamProcessor} objects created by the same factory).
   */
  public ClientStreamProcessor(Address address, ServerStream localStream) {
    this.localStream = localStream;
    this.localContext = ChannelContext.create(address);
    localStream.subscribe(localContext);
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
  public void onNext(ServiceMessage message) {
    localContext.postWrite(message);
  }

  @Override
  public Observable<ServiceMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      // response logic: remote read => onResponse
      subscriptions.add(
          localContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onResponse(message, emitter)));

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
    // from local server stream => no more requests, no more replies
    localContext.close();
  }

  private void onResponse(ServiceMessage message, Observer<ServiceMessage> emitter) {
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
