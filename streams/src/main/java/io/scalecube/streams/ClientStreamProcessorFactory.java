package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.internal.util.SubscriptionList;
import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessorFactory {

  private final ServerStream localEventStream = ServerStream.newServerStream();

  private final SubscriptionList subscriptions = new SubscriptionList();

  /**
   * Constructor for this factory. Right away defines logic for bidirectional communication with respect to client side
   * semantics.
   *
   * @param remoteEventStream given {@link ClientStream} object created and operated somewhere.
   */
  public ClientStreamProcessorFactory(ClientStream remoteEventStream) {
    // request logic: local stream => remote stream
    subscriptions.add(
        localEventStream.listenWrite()
            .subscribe(event -> {
              Address address = event.getAddress(); // remote address
              StreamMessage message = event.getMessageOrThrow(); // message to send
              remoteEventStream.send(address, message);
            }));

    // response logic: remote stream => local stream
    subscriptions.add(
        remoteEventStream.listenReadSuccess()
            .map(Event::getMessageOrThrow)
            .subscribe(message -> localEventStream.send(message, ChannelContext::postReadSuccess)));

    // error logic: failed remote write => local stream
    subscriptions.add(
        remoteEventStream.listenWriteError()
            .subscribe(event -> {
              StreamMessage originalMessage = event.getMessageOrThrow();
              localEventStream.send(originalMessage, (channelContext, message) -> {
                Throwable throwable = event.getErrorOrThrow(); // got write error
                channelContext.postWriteError(message, throwable); // rethrow it
              });
            }));

    // connection logic: connection lost => local stream
    subscriptions.add(
        remoteEventStream.listenChannelContextClosed()
            .subscribe(event -> {
              Address address = event.getAddress(); // address where connection lost
              localEventStream.onNext(address, event); // forward connection lost
            }));
  }

  /**
   * Creates new {@link StreamProcessor} which operates on top client side semantics.
   * 
   * @param address target endpoint address
   * @return stream processor
   */
  public StreamProcessor newClientStreamProcessor(Address address) {
    return new DefaultStreamProcessor(ChannelContext.create(address), localEventStream);
  }

  /**
   * Clears subscriptions and closes local {@link EventStream} (which inherently unsubscribes all subscribed channel
   * contexts on it).
   */
  public void close() {
    subscriptions.clear();
    localEventStream.close();
  }
}
