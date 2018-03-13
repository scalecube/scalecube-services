package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.subscriptions.CompositeSubscription;

import java.net.ConnectException;

public final class ClientStreamProcessor implements StreamProcessor {

  private final ChannelContext localContext;
  private final ChannelContext remoteContext;
  private final ServerStream localStream;

  private final Subscription subscription;

  /**
   * @param address of the target endpoing of where to send request traffic.
   * @param localStream shared serverStream (among {@link ClientStreamProcessor} objects created by the same factory).
   */
  public ClientStreamProcessor(Address address, ServerStream localStream) {
    this.localContext = ChannelContext.create(address);
    this.remoteContext = ChannelContext.create(address);
    this.localStream = localStream;

    // request logic: local context => remote context
    subscription = localContext.listenWrite()
        .map(Event::getMessageOrThrow)
        .subscribe(remoteContext::postWrite);

    // bind remote context
    localStream.subscribe(remoteContext);
  }

  @Override
  public void onCompleted() {
    onNext(onCompletedMessage);
    localContext.close();
  }

  @Override
  public void onError(Throwable throwable) {
    onNext(onErrorMessage);
    localContext.close();
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

      subscriptions.add(
          // response logic: remote read => onResponse
          remoteContext.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onResponse(message, emitter)));

      subscriptions.add(
          // error logic: failed remote write => observer error
          remoteContext.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      subscriptions.add(
          // connection lost logic: unsubscribed => observer error
          localStream.listenChannelContextUnsubscribed()
              .subscribe(event -> emitter.onError(new ConnectException())));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    subscription.unsubscribe();
    localContext.close();
    remoteContext.close();
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
}
