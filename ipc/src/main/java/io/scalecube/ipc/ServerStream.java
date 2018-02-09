package io.scalecube.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class ServerStream implements EventStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  public static final String SENDER_ID_DELIMITER = "/";

  public static final Throwable INVALID_IDENTITY_EXCEPTION =
      new IllegalArgumentException("ServiceMessage: identity is invalid or missing");

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();

  private ServerStream() {}

  /**
   * Return new instance of server stream.
   */
  public static ServerStream newServerStream() {
    return new ServerStream();
  }

  @Override
  public void subscribe(Observable<Event> observable, Consumer<Throwable> onError, Consumer<Void> onCompleted) {
    observable.subscribe(subject::onNext, onError::accept, () -> onCompleted.accept(null));
  }

  @Override
  public void close() {
    subject.onCompleted();
  }

  /**
   * Sends a message to client identified by message's senderId, and applying server stream semantic for outbound
   * messages.
   * 
   * @param message message to send; must contain valid senderId.
   */
  public void send(ServiceMessage message) {
    onSend(message,
        (identity, message1) -> {
          ChannelContext channelContext = ChannelContext.getIfExist(identity);
          if (channelContext == null) {
            LOGGER.warn("Failed to handle message: {}, channel context is null by id: {}", message, identity);
          } else {
            channelContext.postMessageWrite(message1);
          }
        },
        throwable -> LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable));
  }

  /**
   * Sends a message to client identified by message's senderId, and applying server stream semantic for outbound
   * messages.
   *
   * @param message message to send; must contain valid senderId.
   */
  public void send(ServiceMessage message,
      BiConsumer<String, ServiceMessage> consumer0, Consumer<Throwable> consumer1) {
    onSend(message, consumer0, consumer1);
  }

  /**
   * Subscription point for events coming on server channels. Applying server stream semantic for inbound messages.
   */
  @Override
  public Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable().map(event -> {
      Optional<ServiceMessage> message = event.getMessage();
      return message.isPresent()
          ? Event.copyFrom(event).message(onReceive(message.get(), event.getIdentity())).build() // copy and modify
          : event; // pass it through
    });
  }

  private void onSend(ServiceMessage message,
      BiConsumer<String, ServiceMessage> consumer0, Consumer<Throwable> consumer1) {
    if (!message.hasSenderId()
        || message.getSenderId().startsWith(SENDER_ID_DELIMITER)
        || message.getSenderId().endsWith(SENDER_ID_DELIMITER)) {
      consumer1.accept(INVALID_IDENTITY_EXCEPTION);
      return;
    }

    String senderId = message.getSenderId();
    String serverId = senderId;
    String newSenderId = null;
    int delimiter = senderId.lastIndexOf(SENDER_ID_DELIMITER);
    if (delimiter > 0) {
      // extract last identity
      serverId = senderId.substring(delimiter + 1);
      if (serverId.isEmpty()) {
        consumer1.accept(INVALID_IDENTITY_EXCEPTION);
        return;
      }
      // construct new sender qualfier
      newSenderId = senderId.substring(0, delimiter);
      if (newSenderId.isEmpty()) {
        consumer1.accept(INVALID_IDENTITY_EXCEPTION);
        return;
      }
    }

    ServiceMessage message1 = ServiceMessage.copyFrom(message).senderId(newSenderId).build(); // copy and modify
    consumer0.accept(serverId, message1);
  }

  private ServiceMessage onReceive(ServiceMessage message, String identity) {
    String newSenderId = identity;
    if (message.hasSenderId()) {
      newSenderId = message.getSenderId() + SENDER_ID_DELIMITER + identity;
    }
    return ServiceMessage.copyFrom(message).senderId(newSenderId).build();
  }
}
