package io.scalecube.streams.codec;

import static io.scalecube.streams.StreamMessage.DATA_NAME;
import static io.scalecube.streams.StreamMessage.QUALIFIER_NAME;
import static io.scalecube.streams.StreamMessage.SUBJECT_NAME;

import io.scalecube.streams.StreamMessage;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class StreamMessageCodec {

  private static final List<String> FLAT_FIELDS = ImmutableList.of(QUALIFIER_NAME, SUBJECT_NAME);
  private static final List<String> MATCH_FIELDS = ImmutableList.of(DATA_NAME);

  private StreamMessageCodec() {
    // Do not instantiate
  }

  /**
   * Decodes message from ByteBuf.
   * 
   * @param sourceBuf source buffer; reader index of this buffer will stay unchanged.
   * @return message with sliced data buffer extracted from source buffer.
   * @see ByteBufCodec#decode(ByteBuf, List, List, BiConsumer)
   */
  public static StreamMessage decode(ByteBuf sourceBuf) {
    StreamMessage.Builder messageBuilder = StreamMessage.builder();
    try {
      ByteBufCodec.decode(sourceBuf.slice(), FLAT_FIELDS, MATCH_FIELDS, (fieldName, value) -> {
        switch (fieldName) {
          case QUALIFIER_NAME:
            messageBuilder.qualifier((String) value);
            break;
          case SUBJECT_NAME:
            messageBuilder.subject((String) value);
            break;
          case DATA_NAME:
            messageBuilder.data(((ByteBuf) value).retain()); // ByteBuf
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
   * Encodes message to ByteBuf.
   * 
   * @param message to encode
   * @return encoded message
   * @see ByteBufCodec#encode(ByteBuf, List, List, Function)
   */
  public static ByteBuf encode(StreamMessage message) {
    ByteBuf targetBuf = ByteBufAllocator.DEFAULT.buffer();
    try {
      ByteBufCodec.encode(targetBuf, FLAT_FIELDS, MATCH_FIELDS, fieldName -> {
        switch (fieldName) {
          case QUALIFIER_NAME:
            return message.qualifier();
          case SUBJECT_NAME:
            return message.subject();
          case DATA_NAME:
            return message.data(); // ByteBuf
          default:
            return null;
        }
      });
    } catch (Exception e) {
      targetBuf.release(); // buf belongs to this function => he released in this function
      throw new EncoderException(e);
    }
    return targetBuf;
  }
}
