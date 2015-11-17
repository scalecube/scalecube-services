package io.scalecube.transport;

import static io.protostuff.ProtostuffIOUtil.writeTo;

import io.scalecube.transport.utils.RecyclableLinkedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.EncoderException;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

final class ProtostuffMessageSerializer implements MessageSerializer {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

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
