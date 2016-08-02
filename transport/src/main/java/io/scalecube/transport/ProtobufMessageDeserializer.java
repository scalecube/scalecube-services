package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.DecoderException;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public final class ProtobufMessageDeserializer implements MessageDeserializer {

  @Override
  public Message deserialize(ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    Message message = schema.newMessage();
    try {
      ProtobufIOUtil.mergeFrom(new ByteBufInputStream(bb), message, schema);
    } catch (Exception e) {
      throw new DecoderException(e.getMessage(), e);
    }
    return message;
  }
}
