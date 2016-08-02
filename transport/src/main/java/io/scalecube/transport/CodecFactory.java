package io.scalecube.transport;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class CodecFactory {

  private CodecFactory() {}

  public static ChannelHandler protobufFrameDecoder() {
    return new ProtobufVarint32FrameDecoder();
  }

  public static ChannelHandler protobufFrameEncoder() {
    return new ProtobufVarint32LengthFieldPrepender();
  }

  public static ChannelHandler protobufMessageDecoder() {
    return new SharableDeserializerHandler(new ProtobufMessageDeserializer());
  }

  public static ChannelHandler protobufMessageEncoder() {
    return new SharableSerializerHandler(new ProtobufMessageSerializer());
  }
}
