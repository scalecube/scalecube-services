package io.scalecube.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

public final class ServerStream extends DefaultEventStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);

  public static final String SUBJECT_DELIMITER = "/";

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
   * Sends a message to client identified by message's subject, and applying server stream semantic for outbound
   * messages.
   * 
   * @param message message to send; must contain valid subject.
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
   * @param message message to send; must contain valid subject.
   * @param consumer action to proceed with message after figuring out its identity; in the biConsumer first param is a
   *        {@link ChannelContext}, the second - a message to work with further.
   * @param throwableConsumer throwable consumer; in the biConsumer first param is an error occured during message send,
   *        the second - an original message.
   */
  public void send(StreamMessage message,
      BiConsumer<ChannelContext, StreamMessage> consumer, BiConsumer<Throwable, StreamMessage> throwableConsumer) {
    if (!message.containsSubject()
        || message.subject().startsWith(SUBJECT_DELIMITER)
        || message.subject().endsWith(SUBJECT_DELIMITER)) {
      throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
      return;
    }

    String subject = message.subject();
    String lastIdentity = subject;
    String newSubject = null;
    int lastDelimiter = subject.lastIndexOf(SUBJECT_DELIMITER);
    if (lastDelimiter > 0) {
      // extract last identity
      lastIdentity = subject.substring(lastDelimiter + 1);
      if (lastIdentity.isEmpty()) {
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
        return;
      }
      // construct new subject qualfier
      newSubject = subject.substring(0, lastDelimiter);
      if (newSubject.isEmpty()) {
        throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
        return;
      }
    }

    ChannelContext channelContext = ChannelContext.getIfExist(lastIdentity);
    if (channelContext == null) {
      throwableConsumer.accept(INVALID_IDENTITY_EXCEPTION, message);
      return;
    }

    try {
      // copy and modify
      StreamMessage message1 = StreamMessage.from(message).subject(newSubject).build();
      consumer.accept(channelContext, message1);
    } catch (Exception throwable) {
      throwableConsumer.accept(throwable, message);
    }
  }

  private static Event mapEventOnReceive(Event event) {
    if (!event.getMessage().isPresent()) {
      return event; // pass it through
    }
    StreamMessage message = event.getMessageOrThrow();
    String subject = event.getIdentity();
    if (message.containsSubject()) {
      subject = message.subject() + SUBJECT_DELIMITER + subject;
    }
    StreamMessage message1 = StreamMessage.from(message).subject(subject).build();
    return Event.copyFrom(event).message(message1).build(); // copy and modify
  }

  private void handleFailedMessageSend(Throwable throwable, StreamMessage message) {
    LOGGER.warn("Failed to send {} on server stream, cause: {}", message, throwable);
  }
}
