package io.servicefabric.transport.protocol;

/**
 * @author Anton Kharenko
 */
public interface Protocol {

  FrameHandlerFactory getFrameHandlerFactory();

  MessageDeserializer getMessageDeserializer();

  MessageSerializer getMessageSerializer();

}
