package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.Emitter;
import rx.Observable;
import rx.Subscription;
import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessor implements StreamProcessor {

  private final ChannelContext localContext;
  private final ChannelContext remoteContext;

  private final Subscription subscription;

  /**
   * @param address of the target endpoing of where to send request traffic.
   * @param serverStream shared {@link ServerStream}.
   */
  private ClientStreamProcessor(Address address, ServerStream serverStream) {
    localContext = ChannelContext.create(address);

    // bind remote context
    serverStream.subscribe(remoteContext = ChannelContext.create(address));

    // request logic
    subscription = localContext.listenWrite()
        .map(Event::getMessageOrThrow)
        .subscribe(remoteContext::postWrite);
  }

  /**
   * Factory method. Creates new {@link Factory} on the given clientStream.
   */
  public static Factory newFactory(ClientStream clientStream) {
    return new Factory(clientStream);
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

      subscriptions.add(remoteContext.listenReadSuccess()
          .map(Event::getMessageOrThrow)
          .flatMap(this::toResponse)
          .subscribe(emitter));

      subscriptions.add(remoteContext.listenWriteError()
          .map(Event::getErrorOrThrow)
          .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    localContext.close();
    remoteContext.close();
    subscription.unsubscribe();
  }

  private Observable<? extends ServiceMessage> toResponse(ServiceMessage message) {
    String qualifier = message.getQualifier();
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier)) { // remote => onCompleted
      return Observable.empty();
    }
    String qualifierNamespace = Qualifier.getQualifierNamespace(qualifier);
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onError
      // Hint: at this point more sophisticated exception mapping logic is needed
      return Observable.error(new RuntimeException(qualifier));
    }
    return Observable.just(message); // remote => normal response
  }

  /**
   * Factory for creating {@link ClientStreamProcessor} instances.
   */
  public static final class Factory {

    private final ServerStream localStream = ServerStream.newServerStream();

    private final CompositeSubscription subscriptions = new CompositeSubscription();

    /**
     * Constructor for {@link Factory}. Right away defines shared logic (across all of {@link ClientStreamProcessor}
     * objects) for bidirectional communication with respect to client side semantics.
     *
     * @param remoteStream injected {@link ClientStream}; factory object wouldn't close it in {@link #close()} method.
     */
    private Factory(ClientStream remoteStream) {
      // request logic
      subscriptions.add(localStream.listenWrite()
          .subscribe(event -> remoteStream.send(event.getAddress(), event.getMessageOrThrow())));

      // response logic
      subscriptions.add(remoteStream.listenReadSuccess()
          .map(Event::getMessageOrThrow)
          .subscribe(message -> localStream.send(message, ChannelContext::postReadSuccess)));

      // response logic
      subscriptions.add(remoteStream.listenWriteError()
          .subscribe(event -> localStream.send(event.getMessageOrThrow(), (channelContext, message1) -> {
            Address address = event.getAddress();
            Throwable throwable = event.getErrorOrThrow();
            channelContext.postWriteError(address, message1, throwable);
          })));
    }

    /**
     * Creates new {@link ClientStreamProcessor} per address. This is address of the target endpoing of where to send
     * request traffic.
     */
    public ClientStreamProcessor newStreamProcessor(Address address) {
      return new ClientStreamProcessor(address, localStream);
    }

    /**
     * Closes internal shared serverStream (across all of {@link ClientStreamProcessor} objects) and cleanups
     * subscriptions.
     */
    public void close() {
      localStream.close();
      subscriptions.clear();
    }
  }
}
