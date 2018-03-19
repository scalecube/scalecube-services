package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.subscriptions.CompositeSubscription;

import java.util.function.Consumer;

public final class ServerStreamProcessorFactory {

  private final ListeningServerStream remoteEventStream;
  private final DefaultEventStream localEventStream = new DefaultEventStream();

  private final Consumer<StreamProcessor> streamProcessorConsumer;

  private final CompositeSubscription subscriptions = new CompositeSubscription();

  /**
   * Constructor for this factory. Right away defines logic for bidirectional communication with respect to server side
   * semantics.
   * 
   * @param remoteEventStream given {@link ServerStream} object created and operated somewhere.
   * @param streamProcessorConsumer consumer of {@link StreamProcessor} instance.
   */
  private ServerStreamProcessorFactory(ListeningServerStream remoteEventStream,
      Consumer<StreamProcessor> streamProcessorConsumer) {
    this.remoteEventStream = remoteEventStream;
    this.streamProcessorConsumer = streamProcessorConsumer;

    // request logic: remote stream => local stream
    subscriptions.add(
        remoteEventStream.listenReadSuccess()
            .subscribe(event -> {
              Address address = event.getAddress();
              StreamMessage message = event.getMessageOrThrow();
              String id = message.getSenderId();
              // create channelContext if needed
              ChannelContext channelContext =
                  ChannelContext.createIfAbsent(id, address, this::initChannelContext);
              // forward read
              channelContext.postReadSuccess(message);
            }));

    // connection logic: connection lost => local stream
    subscriptions.add(
        remoteEventStream.listenChannelContextClosed()
            .subscribe(event -> localEventStream.onNext(event.getAddress(), event)));
  }

  private void initChannelContext(ChannelContext channelContext) {
    // response logic: local write => remote stream
    channelContext.listenWrite()
        .map(event -> {
          String id = channelContext.getId();
          StreamMessage message = event.getMessageOrThrow();
          // reset outgoing message identity with channelContext's identity
          return StreamMessage.copyFrom(message).senderId(id).build();
        })
        .subscribe(remoteEventStream::send);

    streamProcessorConsumer.accept(new DefaultStreamProcessor(channelContext, localEventStream));

    // bind channel context
    localEventStream.subscribe(channelContext);
  }

  /**
   * Creates stream processor factory.
   * 
   * @param remoteEventStream server stream created somewhere; this stream is a source for incoming events upon which
   *        factory will apply its processing logic and server side semantics.
   * @param streamProcessorConsumer consumer of {@link StreamProcessor} instance for integration with application level;
   *        this is an integration point for functionality defined on top of this factory.
   * @return stream processor factory
   * @see #ServerStreamProcessorFactory(ListeningServerStream, Consumer)
   */
  public static ServerStreamProcessorFactory newServerStreamProcessorFactory(ListeningServerStream remoteEventStream,
      Consumer<StreamProcessor> streamProcessorConsumer) {
    return new ServerStreamProcessorFactory(remoteEventStream, streamProcessorConsumer);
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
