package io.servicefabric.transport;

/**
 * Represents protocol for transport.
 * Protocol consist of frame handler {@link io.servicefabric.transport.FrameHandlerFactory},
 * message serializer {@link io.servicefabric.transport.MessageSerializer},
 * message deserializer {@link io.servicefabric.transport.MessageDeserializer}
 * @author Anton Kharenko
 */
public interface Protocol {

  FrameHandlerFactory getFrameHandlerFactory();

  MessageDeserializer getMessageDeserializer();

  MessageSerializer getMessageSerializer();

}
