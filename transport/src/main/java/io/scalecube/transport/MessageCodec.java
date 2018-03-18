package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Contains static methods for message serializing/deserializing logic.
 *
 * @author Anton Kharenko
 */
public final class MessageCodec {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  static {
    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class)) {
      RuntimeSchema.register(Message.class, new MessageSchema());
    }
  }

  private MessageCodec() {
    // Do not instantiate
  }

  /**
   * Deserializes message from given byte buffer.
   *
   * @param bb byte buffer
   */
  public static Message deserialize(ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    Message message = schema.newMessage();
    try {
      ProtostuffIOUtil.mergeFrom(new ByteBufInputStream(bb), message, schema);
    } catch (Exception e) {
      throw new DecoderException(e.getMessage(), e);
    }

    return message;
  }

  /**
   * Serializes given message into byte buffer.
   *
   * @param message message to serialize
   * @param bb byte buffer of where to write serialzied message
   */
  public static void serialize(Message message, ByteBuf bb) {
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
