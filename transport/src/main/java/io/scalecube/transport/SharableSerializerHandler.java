package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

@ChannelHandler.Sharable
final class SharableSerializerHandler extends MessageToByteEncoder<Message> {

  private final MessageSerializer serializer;

  public SharableSerializerHandler(MessageSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
    serializer.serialize(msg, out);
  }
}
