package io.servicefabric.transport.protocol;

import static io.protostuff.ProtostuffIOUtil.writeTo;

import io.servicefabric.transport.utils.RecyclableLinkedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.EncoderException;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public final class ProtostuffMessageSerializer implements MessageSerializer {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  // TODO [Anton Kharenko]: Move to more appropriate place
  static {
    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class))
      RuntimeSchema.register(Message.class, new MessageSchema());
  }

  @Override
  public void serialize(Message message, ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      try {
        writeTo(new ByteBufOutputStream(bb), message, schema, rlb.buffer());
      } catch (Exception e) {
        throw new EncoderException(e.getMessage(), e);
      }
    }
  }

}
