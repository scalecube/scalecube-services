package io.servicefabric.transport.protocol.protostuff;

import io.servicefabric.transport.protocol.FrameHandlerFactory;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public final class ProtostuffFrameHandlerFactory implements FrameHandlerFactory {

	@Override
	public ByteToMessageDecoder getFrameDecoder() {
		return new ProtobufVarint32FrameDecoder();
	}

	@Override
	public MessageToByteEncoder getFrameEncoder() {
		return new ProtobufVarint32LengthFieldPrepender();
	}
}
