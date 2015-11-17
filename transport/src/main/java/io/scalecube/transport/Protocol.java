package io.scalecube.transport;

/**
 * Represents protocol for transport.
 * Protocol consist of frame handler {@link FrameHandlerFactory},
 * message serializer {@link MessageSerializer},
 * message deserializer {@link MessageDeserializer}
 * @author Anton Kharenko
 */
public interface Protocol {

  FrameHandlerFactory getFrameHandlerFactory();

  MessageDeserializer getMessageDeserializer();

  MessageSerializer getMessageSerializer();

}
