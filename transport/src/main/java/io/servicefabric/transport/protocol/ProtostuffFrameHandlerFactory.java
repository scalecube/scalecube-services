package io.servicefabric.transport.protocol;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

final class ProtostuffFrameHandlerFactory implements FrameHandlerFactory {

  @Override
  public ByteToMessageDecoder newFrameDecoder() {
    return new ProtobufVarint32FrameDecoder();
  }

  @Override
  public MessageToByteEncoder newFrameEncoder() {
    return new ProtobufVarint32LengthFieldPrepender();
  }
}
