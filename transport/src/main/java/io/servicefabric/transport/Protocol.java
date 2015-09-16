package io.servicefabric.transport;

/**
 * @author Anton Kharenko
 */
public interface Protocol {

  FrameHandlerFactory getFrameHandlerFactory();

  MessageDeserializer getMessageDeserializer();

  MessageSerializer getMessageSerializer();

}
