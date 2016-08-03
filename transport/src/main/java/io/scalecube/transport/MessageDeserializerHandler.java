package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

@ChannelHandler.Sharable
final class MessageDeserializerHandler extends MessageToMessageDecoder<ByteBuf> {

  private final MessageDeserializer deserializer = new MessageDeserializer();

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    out.add(deserializer.deserialize(msg));
  }
}
