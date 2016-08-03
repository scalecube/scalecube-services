package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

@ChannelHandler.Sharable
final class SharableDeserializerHandler extends MessageToMessageDecoder<ByteBuf> {

  private final MessageDeserializer deserializer;

  public SharableDeserializerHandler(MessageDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
    Message message = deserializer.deserialize(msg);
    out.add(message);
  }
}
