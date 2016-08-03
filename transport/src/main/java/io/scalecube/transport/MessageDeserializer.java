package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.DecoderException;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public final class MessageDeserializer {

  static {
    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class)) {
      RuntimeSchema.register(Message.class, new MessageSchema());
    }
  }

  /**
   * Deserializes message from given byte buffer.
   */
  public Message deserialize(ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    Message message = schema.newMessage();
    try {
      ProtostuffIOUtil.mergeFrom(new ByteBufInputStream(bb), message, schema);
    } catch (Exception e) {
      throw new DecoderException(e.getMessage(), e);
    }

    return message;
  }

}
