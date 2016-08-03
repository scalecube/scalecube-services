package io.scalecube.transport;

import io.scalecube.transport.utils.RecyclableLinkedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.EncoderException;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public final class MessageSerializer {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  static {
    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class)) {
      RuntimeSchema.register(Message.class, new MessageSchema());
    }
  }

  /**
   * Serializes given message into byte buffer.
   */
  public void serialize(Message message, ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      try {
        ProtostuffIOUtil.writeTo(new ByteBufOutputStream(bb), message, schema, rlb.buffer());
      } catch (Exception e) {
        throw new EncoderException(e.getMessage(), e);
      }
    }
  }

}
