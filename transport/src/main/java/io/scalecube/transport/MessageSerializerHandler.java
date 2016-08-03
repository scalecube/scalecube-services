package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

@ChannelHandler.Sharable
final class MessageSerializerHandler extends MessageToByteEncoder<Message> {

  private final MessageSerializer serializer = new MessageSerializer();

  @Override
  protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
    serializer.serialize(msg, out);
  }
}
