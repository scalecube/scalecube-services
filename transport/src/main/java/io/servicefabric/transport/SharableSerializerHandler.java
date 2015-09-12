package io.servicefabric.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageSerializer;

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
