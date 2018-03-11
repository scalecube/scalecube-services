package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.subscriptions.CompositeSubscription;

public final class ServerStreamProcessorFactory {

  private final ServerStream remoteStream;
  private final DefaultEventStream localStream;

  private final CompositeSubscription subscriptions = new CompositeSubscription();

  /**
   * Constructor for {@link ServerStreamProcessorFactory}. Right away defines logic for bidirectional communication with
   * respect to server side semantics.
   *
   * @param remoteStream injected {@link ServerStream}; factory wouldn't close it in {@link #close()} method.
   * @param localStream injected {@link DefaultEventStream}; factory wouldn't close it in {@link #close()} method.
   */
  private ServerStreamProcessorFactory(ServerStream remoteStream, DefaultEventStream localStream) {
    this.remoteStream = remoteStream;
    this.localStream = localStream;

    // request logic: remote stream => local stream
    subscriptions.add(
        remoteStream.listenReadSuccess()
            .subscribe(event -> {
              Address address = event.getAddress();
              ServiceMessage message = event.getMessageOrThrow();
              String id = message.getSenderId();
              // create channelContext if needed
              ChannelContext channelContext =
                  ChannelContext.createIfAbsent(id, address, this::init);
              // forward read
              channelContext.postReadSuccess(message);
            }));

    // connection lost logic: unsubscribe by address
    subscriptions.add(
        remoteStream.listenChannelContextClosed()
            .map(Event::getAddress)
            .subscribe(localStream::unsubscribe));
  }

  private void init(ChannelContext channelContext) {
    // response logic: local write => remote stream
    channelContext.listenWrite()
        .map(event -> {
          String id = channelContext.getId();
          ServiceMessage message = event.getMessageOrThrow();
          return ServiceMessage.copyFrom(message).senderId(id).build();
        })
        .subscribe(remoteStream::send);

    // bind channel context
    localStream.subscribe(channelContext);
  }

  /**
   * Creates new {@link ServerStreamProcessorFactory} on the given {@link DefaultEventStream} and {@link ServerStream}.
   */
  public static ServerStreamProcessorFactory newServerStreamProcessorFactory(ServerStream remoteStream,
      DefaultEventStream localStream) {
    return new ServerStreamProcessorFactory(remoteStream, localStream);
  }

  public void close() {
    subscriptions.clear();
  }
}
