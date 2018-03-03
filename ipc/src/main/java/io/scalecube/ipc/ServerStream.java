package io.scalecube.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class ServerStream extends DefaultEventStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  public static final String SENDER_ID_DELIMITER = "/";

  public static final Throwable INVALID_IDENTITY_EXCEPTION =
      new IllegalArgumentException("ServiceMessage: identity is invalid or missing");

  private ServerStream() {
    super(ServerStream::mapEventOnReceive);
  }

  /**
   * Return new instance of server stream.
   */
  public static ServerStream newServerStream() {
    return new ServerStream();
  }

  /**
   * Sends a message to client identified by message's senderId, and applying server stream semantic for outbound
   * messages.
   * 
   * @param message message to send; must contain valid senderId.
   */
  public void send(ServiceMessage message) {
    send(message,
        (identity, message1) -> ChannelContext.getIfExist(identity).postMessageWrite(message1),
        throwable -> LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable));
  }

  /**
   * This method applies server stream semantic for outbound messages and giving mechanism to react on successful and
   * unsuccessfull outcomes.
   * 
   * @param message message to send; must contain valid senderId.
   * @param consumer0 action to proceed with message after figuring out its identity; in the biConsumer first param is
   *        extracted identity, second - a message to work with further.
   * @param consumer1 throwable consumer.
   */
  public void send(ServiceMessage message,
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

    try {
      // copy and modify
      ServiceMessage message1 = ServiceMessage.copyFrom(message).senderId(newSenderId).build();
      consumer0.accept(serverId, message1);
    } catch (Exception throwable) {
      consumer1.accept(throwable);
    }
  }

  private static Event mapEventOnReceive(Event event) {
    Optional<ServiceMessage> message = event.getMessage();
    if (!message.isPresent()) {
      return event; // pass it through
    }
    String senderId = event.getIdentity();
    if (message.get().hasSenderId()) {
      senderId = message.get().getSenderId() + SENDER_ID_DELIMITER + event.getIdentity();
    }
    ServiceMessage message1 = ServiceMessage.copyFrom(message.get()).senderId(senderId).build();
    return Event.copyFrom(event).message(message1).build(); // copy and modify
  }
}
