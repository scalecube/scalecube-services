package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.DecoderException;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

final class ProtostuffMessageDeserializer implements MessageDeserializer {

  @Override
  public Message deserialize(ByteBuf bb) {
    // Deserialize BinaryMessage
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
