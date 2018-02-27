package io.scalecube.ipc.codec;

import static io.scalecube.ipc.ServiceMessage.DATA_NAME;
import static io.scalecube.ipc.ServiceMessage.QUALIFIER_NAME;
import static io.scalecube.ipc.ServiceMessage.SENDER_ID_NAME;
import static io.scalecube.ipc.ServiceMessage.STREAM_ID_NAME;

import io.scalecube.ipc.ServiceMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.util.List;

public final class ServiceMessageCodec {

  private static final List<String> FLAT_FIELDS = ImmutableList.of(QUALIFIER_NAME, SENDER_ID_NAME, STREAM_ID_NAME);
  private static final List<String> MATCH_FIELDS = ImmutableList.of(DATA_NAME);

  private ServiceMessageCodec() {
    // Do not instantiate
  }

  /**
   * Takes a buffer and.
   * 
   * @param buf source buffer; reader index of this buffer will stay unchanged.
   * @return service message object with sliced data buffer extracted from source buffer.
   */
  public static ServiceMessage decode(ByteBuf buf) {
    ServiceMessage.Builder messageBuilder = ServiceMessage.builder();
    try {
      JsonCodec.decode(buf.slice(), FLAT_FIELDS, MATCH_FIELDS, (fieldName, value) -> {
        switch (fieldName) {
          case QUALIFIER_NAME:
            messageBuilder.qualifier((String) value);
            break;
          case SENDER_ID_NAME:
            messageBuilder.senderId((String) value);
            break;
          case STREAM_ID_NAME:
            messageBuilder.streamId((String) value);
            break;
          case DATA_NAME:
            messageBuilder.data(value); // ByteBuf
            break;
          default:
            // no-op
        }
      });
    } catch (Exception e) {
      throw new DecoderException(e);
    }
    return messageBuilder.build();
  }

  /**
   * Encode message to byte buffer.
   * 
   * @param message to encode to byte buffer.
   * @return message as byte buffer.
   */
  public static ByteBuf encode(ServiceMessage message) {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
    try {
      JsonCodec.encode(buf, FLAT_FIELDS, MATCH_FIELDS, fieldName -> {
        switch (fieldName) {
          case QUALIFIER_NAME:
            return message.getQualifier();
          case SENDER_ID_NAME:
            return message.getSenderId();
          case STREAM_ID_NAME:
            return message.getStreamId();
          case DATA_NAME:
            return message.getData(); // ByteBuf
          default:
            return null;
        }
      });
    } catch (Exception e) {
      buf.release(); // buf belongs to this function => he released in this function
      throw new EncoderException(e);
    }
    return buf;
  }
}
