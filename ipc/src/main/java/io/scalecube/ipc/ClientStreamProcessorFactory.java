package io.scalecube.ipc;

import io.scalecube.transport.Address;

import rx.subscriptions.CompositeSubscription;

public final class ClientStreamProcessorFactory {

  private final ServerStream localStream = ServerStream.newServerStream();

  private final CompositeSubscription subscriptions = new CompositeSubscription();

  /**
   * Constructor for this factory. Right away defines logic for bidirectional communication with respect to client side
   * semantics.
   *
   * @param remoteStream injected {@link ClientStream}; factory wouldn't close it in {@link #close()} method.
   */
  private ClientStreamProcessorFactory(ClientStream remoteStream) {
    // request logic: local stream => remote stream
    subscriptions.add(
        localStream.listenWrite()
            .subscribe(event -> remoteStream.send(event.getAddress(), event.getMessageOrThrow())));

    // response logic: remote stream => local stream
    subscriptions.add(
        remoteStream.listenReadSuccess()
            .map(Event::getMessageOrThrow)
            .subscribe(message -> localStream.send(message, ChannelContext::postReadSuccess)));

    // error logic: failed remote write => local stream
    subscriptions.add(
        remoteStream.listenWriteError()
            .subscribe(event -> localStream.send(event.getMessageOrThrow(), (channelContext, message1) -> {
              Address address = event.getAddress();
              Throwable throwable = event.getErrorOrThrow();
              channelContext.postWriteError(address, message1, throwable);
            })));

    // connection logic: connection lost => local stream
    subscriptions.add(
        remoteStream.listenChannelContextClosed()
            .subscribe(event -> localStream.onNext(event.getAddress(), event)));
  }

  /**
   * Creates stream processor factory.
   * 
   * @param clientStream client stream defined and created somewhere
   * @return stream processor factory
   * @see #ClientStreamProcessorFactory(ClientStream)
   */
  public static ClientStreamProcessorFactory newClientStreamProcessorFactory(ClientStream clientStream) {
    return new ClientStreamProcessorFactory(clientStream);
  }

  /**
   * Creates new {@link ClientStreamProcessor}.
   * 
   * @param address target endpoint address
   * @return stream processor
   */
  public ClientStreamProcessor newClientStreamProcessor(Address address) {
    return new ClientStreamProcessor(address, localStream);
  }

  /**
   * Closes internal shared serverStream (among {@link ClientStreamProcessor} objects created by the same factory).
   */
  public void close() {
    subscriptions.clear();
    localStream.close();
  }
}
