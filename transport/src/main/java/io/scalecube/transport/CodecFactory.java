package io.scalecube.transport;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class CodecFactory {

  private CodecFactory() {}

  public static ChannelHandler protoFrameDecoder() {
    return new ProtobufVarint32FrameDecoder();
  }

  public static ChannelHandler protoFrameEncoder() {
    return new ProtobufVarint32LengthFieldPrepender();
  }

  public static ChannelHandler protostuffMessageDecoder() {
    return new SharableDeserializerHandler(new ProtostuffMessageDeserializer());
  }

  public static ChannelHandler protostuffMessageEncoder() {
    return new SharableSerializerHandler(new ProtostuffMessageSerializer());
  }
}
