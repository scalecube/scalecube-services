package io.scalecube.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;

public final class ServerStream extends DefaultEventStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  public static final String SENDER_ID_DELIMITER = "/";

  public static final Throwable INVALID_IDENTITY_EXCEPTION =
      new IllegalArgumentException("StreamMessage: identity is invalid or missing");

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
  public void send(StreamMessage message) {
    send(message, ChannelContext::postWrite, this::handleFailedMessageSend);
  }

  /**
   * This method applies server stream semantic for outbound messages and giving mechanism to react on successful and
   * unsuccessfull outcomes.
   */
  public void send(StreamMessage message, BiConsumer<ChannelContext, StreamMessage> consumer) {
    send(message, consumer, this::handleFailedMessageSend);
  }

  /**
   * This method applies server stream semantic for outbound messages and giving mechanism to react on successful and
   * unsuccessfull outcomes.
   * 
   * @param message message to send; must contain valid senderId.
   * @param consumer action to proceed with message after figuring out its identity; in the biConsumer first param is a
   *        {@link ChannelContext}, the second - a message to work with further.
   * @param throwableConsumer throwable consumer; in the biConsumer first param is an error occured during message send,
   *        the second - an original message.
   */
  public void send(StreamMessage message,
      BiConsumer<ChannelContext, StreamMessage> consumer, BiConsumer<Throwable, StreamMessage> throwableConsumer) {
    if (!message.hasSenderId()
        || message.getSenderId().startsWith(SENDER_ID_DELIMITER)
        || message.getSenderId().endsWith(SENDER_ID_DELIMITER)) {
      throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
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
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
        return;
      }
      // construct new sender qualfier
      newSenderId = senderId.substring(0, delimiter);
      if (newSenderId.isEmpty()) {
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
        return;
      }
    }

    ChannelContext channelContext = ChannelContext.getIfExist(serverId);
    if (channelContext == null) {
      throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
      return;
    }

    try {
      // copy and modify
      StreamMessage message1 = StreamMessage.copyFrom(message).senderId(newSenderId).build();
      consumer.accept(channelContext, message1);
    } catch (Exception throwable) {
      throwableConsumer.accept(throwable, message);
    }
  }

  private static Event mapEventOnReceive(Event event) {
    Optional<StreamMessage> message = event.getMessage();
    if (!message.isPresent()) {
      return event; // pass it through
    }
    String senderId = event.getIdentity();
    if (message.get().hasSenderId()) {
      senderId = message.get().getSenderId() + SENDER_ID_DELIMITER + event.getIdentity();
    }
    StreamMessage message1 = StreamMessage.copyFrom(message.get()).senderId(senderId).build();
    return Event.copyFrom(event).message(message1).build(); // copy and modify
  }

  private void handleFailedMessageSend(Throwable throwable, StreamMessage message) {
    LOGGER.warn("Failed to send {} on server stream, cause: {}", message, throwable);
  }
}
