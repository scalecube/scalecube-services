package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessorFactory {

  private final ServerStream localEventStream = ServerStream.newServerStream();

  private final CompositeSubscription subscriptions = new CompositeSubscription();

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
            .subscribe(event -> remoteEventStream.send(event.getAddress(), event.getMessageOrThrow())));

    // response logic: remote stream => local stream
    subscriptions.add(
        remoteEventStream.listenReadSuccess()
            .map(Event::getMessageOrThrow)
            .subscribe(message -> localEventStream.send(message, ChannelContext::postReadSuccess)));

    // error logic: failed remote write => local stream
    subscriptions.add(
        remoteEventStream.listenWriteError()
            .subscribe(event -> localEventStream.send(event.getMessageOrThrow(), (channelContext, message1) -> {
              Address address = event.getAddress();
              Throwable throwable = event.getErrorOrThrow();
              String id = channelContext.getId();

              Event.Builder builder = new Event.Builder(Event.Topic.WriteError, address, id);
              Event event1 = builder.error(throwable).message(message1).build();

              channelContext.onNext(event1);
            })));

    // connection logic: connection lost => local stream
    subscriptions.add(
        remoteEventStream.listenChannelContextClosed()
            .subscribe(event -> localEventStream.onNext(event.getAddress(), event)));
  }

  /**
   * Creates new {@link StreamProcessor} which operates on top client side semantics.
   * 
   * @param address target endpoint address
   * @return stream processor
   */
  public StreamProcessor newClientStreamProcessor(Address address) {
    ChannelContext channelContext = ChannelContext.create(address);
    localEventStream.subscribe(channelContext);
    return new DefaultStreamProcessor(channelContext, localEventStream);
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
