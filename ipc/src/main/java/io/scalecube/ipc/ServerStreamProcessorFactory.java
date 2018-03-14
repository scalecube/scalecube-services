package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.subscriptions.CompositeSubscription;

import java.util.function.Consumer;

public final class ServerStreamProcessorFactory {

  private final ServerStream remoteStream;
  private final DefaultEventStream localStream = new DefaultEventStream();
  private final Consumer<ServerStreamProcessor> streamProcessorConsumer;

  private final CompositeSubscription subscriptions = new CompositeSubscription();

  /**
   * Constructor for this factory. Right away defines logic for bidirectional communication with respect to server side
   * semantics.
   * 
   * @param remoteStream injected {@link ServerStream}; factory wouldn't close it in {@link #close()} method.
   * @param streamProcessorConsumer consumer of {@link ServerStreamProcessor} instance.
   */
  private ServerStreamProcessorFactory(ServerStream remoteStream,
      Consumer<ServerStreamProcessor> streamProcessorConsumer) {
    this.remoteStream = remoteStream;
    this.streamProcessorConsumer = streamProcessorConsumer;

    // request logic: remote stream => local stream
    subscriptions.add(
        remoteStream.listenReadSuccess()
            .subscribe(event -> {
              Address address = event.getAddress();
              ServiceMessage message = event.getMessageOrThrow();
              String id = message.getSenderId();
              // create channelContext if needed
              ChannelContext channelContext =
                  ChannelContext.createIfAbsent(id, address, this::initChannelContext);
              // forward read
              channelContext.postReadSuccess(message);
            }));

    // connection logic: connection lost => local stream
    subscriptions.add(
        remoteStream.listenChannelContextClosed()
            .subscribe(event -> localStream.onNext(event.getAddress(), event)));
  }

  private void initChannelContext(ChannelContext channelContext) {
    // response logic: local write => remote stream
    channelContext.listenWrite()
        .map(event -> {
          String id = channelContext.getId();
          ServiceMessage message = event.getMessageOrThrow();
          // reset outgoing message identity with channelContext's identity
          return ServiceMessage.copyFrom(message).senderId(id).build();
        })
        .subscribe(remoteStream::send);

    streamProcessorConsumer.accept(new ServerStreamProcessor(channelContext, localStream));

    // bind channel context
    localStream.subscribe(channelContext);
  }

  /**
   * Creates stream processor factory.
   * 
   * @param remoteStream server stream created somewhere; this stream is a source for incoming events upon which factory
   *        will apply its processing logic and server side semantics.
   * @param streamProcessorConsumer consumer of {@link ServerStreamProcessor} instance for integration with application
   *        level; this is an integration point for functionality defined on top of this factory.
   * @return stream processor factory
   * @see #ServerStreamProcessorFactory(ServerStream, Consumer)
   */
  public static ServerStreamProcessorFactory newServerStreamProcessorFactory(ServerStream remoteStream,
      Consumer<ServerStreamProcessor> streamProcessorConsumer) {
    return new ServerStreamProcessorFactory(remoteStream, streamProcessorConsumer);
  }

  public void close() {
    subscriptions.clear();
    localStream.close();
  }
}
