package io.servicefabric.transport.protocol;

import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Anton Kharenko
 */
public class ProtostuffProtocol implements Protocol {

  private final ProtostuffFrameHandlerFactory frameHandlerFactory;
  private final ProtostuffMessageDeserializer messageDeserializer;
  private final ProtostuffMessageSerializer messageSerializer;

  public ProtostuffProtocol() {
    frameHandlerFactory = new ProtostuffFrameHandlerFactory();
    messageDeserializer = new ProtostuffMessageDeserializer();
    messageSerializer = new ProtostuffMessageSerializer();

    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class)) {
      RuntimeSchema.register(Message.class, new MessageSchema());
    }
  }

  @Override
  public FrameHandlerFactory getFrameHandlerFactory() {
    return frameHandlerFactory;
  }

  @Override
  public MessageDeserializer getMessageDeserializer() {
    return messageDeserializer;
  }

  @Override
  public MessageSerializer getMessageSerializer() {
    return messageSerializer;
  }
}
