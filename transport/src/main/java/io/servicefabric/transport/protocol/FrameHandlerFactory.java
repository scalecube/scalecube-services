package io.servicefabric.transport.protocol;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

public interface FrameHandlerFactory {

  ByteToMessageDecoder newFrameDecoder();

  MessageToByteEncoder newFrameEncoder();

}
