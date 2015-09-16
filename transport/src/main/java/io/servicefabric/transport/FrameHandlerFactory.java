package io.servicefabric.transport;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

public interface FrameHandlerFactory {

  ByteToMessageDecoder newFrameDecoder();

  MessageToByteEncoder newFrameEncoder();

}
