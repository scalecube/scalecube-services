package io.scalecube.ipc;

import rx.Observable;
import rx.Subscription;
import rx.subscriptions.CompositeSubscription;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ServerStreamProcessor implements StreamProcessor {

  private final ChannelContext localContext;
  // private final ServerStream localServerStream;

  private final Subscription subscription;

  public ServerStreamProcessor(String id, ServerStream serverStream) {
    serverStream.subscribe(localContext = ChannelContext.createOrGet(id));

    // request logic
    subscription = localServerStream.listenWrite()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);
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
    return null;
  }

  @Override
  public void close() {

  }

  /**
   * Factory for creating {@link ServerStreamProcessor} instances.
   */
  public static final class Factory {

    private static final ConcurrentMap<String, ServerStreamProcessor> idToStreamProcessor = new ConcurrentHashMap<>();

    private final ServerStream localStream = ServerStream.newServerStream();

    private final CompositeSubscription subscriptions = new CompositeSubscription();

    /**
     * @param remoteStream injected {@link ServerStream}; factory object wouldn't close it in {@link #close()} method.
     */
    private Factory(ServerStream remoteStream) {
      // // request logic
      // subscriptions.add(remoteStream.listenReadSuccess()
      // .map(Event::getMessageOrThrow)
      // .subscribe(message -> localStream.send(message, ChannelContext::postReadSuccess)));

      // response logic
      subscriptions.add(localStream.listenWrite()
          .map(Event::getMessageOrThrow)
          .subscribe(remoteStream::send));
    }

    public ServerStreamProcessor newStreamProcessor(String id) {
      return idToStreamProcessor.computeIfAbsent(id, id1 -> {
        return new ServerStreamProcessor(id1, localStream);
      });
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
