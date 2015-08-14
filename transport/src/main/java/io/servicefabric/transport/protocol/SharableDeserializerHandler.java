package io.servicefabric.transport.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

@ChannelHandler.Sharable
public final class SharableDeserializerHandler extends MessageToMessageDecoder<ByteBuf> {

	private final MessageDeserializer deserializer;

	public SharableDeserializerHandler(MessageDeserializer deserializer) {
		this.deserializer = deserializer;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
		Message message = deserializer.deserialize(msg);
		out.add(message);
	}
}
