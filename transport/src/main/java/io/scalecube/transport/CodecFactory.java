package io.scalecube.transport;

import com.google.common.base.Throwables;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.List;

public class CodecFactory {

  static {
    try {
      Class.forName(MessageSchema.class.getName());
    } catch (ClassNotFoundException cnfe) {
      Throwables.propagate(cnfe);
    }
  }

  private final MessageSerializer serializer = new ProtostuffMessageSerializer();
  private final MessageDeserializer deserializer = new ProtostuffMessageDeserializer();

  public CodecFactory() {}

  /**
   * @return instance of {@link ProtobufVarint32FrameDecoder}.
   */
  public ChannelHandler protoFrameDecoder() {
    return new ProtobufVarint32FrameDecoder();
  }

  /**
   * @return instance of {@link ProtobufVarint32LengthFieldPrepender}.
   */
  public ChannelHandler protoFrameEncoder() {
    return new ProtobufVarint32LengthFieldPrepender();
  }

  /**
   * @return wrapped instance of {@link MessageDeserializer}.
   */
  public ChannelHandler protostuffMessageDecoder() {
    return new MessageToMessageDecoder<ByteBuf>() {
      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        out.add(deserializer.deserialize(msg));
      }
    };
  }

  /**
   * @return wrapped instance of {@link MessageSerializer}.
   */
  public ChannelHandler protostuffMessageEncoder() {
    return new MessageToByteEncoder<Message>() {
      @Override
      protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        serializer.serialize(msg, out);
      }
    };
  }
}
