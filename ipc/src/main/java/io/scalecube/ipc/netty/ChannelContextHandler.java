package io.scalecube.ipc.netty;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.ipc.ChannelContext;
import io.scalecube.transport.Address;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

@Sharable
public final class ChannelContextHandler extends ChannelInboundHandlerAdapter {

  private final Consumer<ChannelContext> channelContextConsumer;

  public ChannelContextHandler(Consumer<ChannelContext> channelContextConsumer) {
    this.channelContextConsumer = channelContextConsumer;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    Channel channel = ctx.channel();
    Attribute<ChannelContext> attribute = channel.attr(ChannelSupport.CHANNEL_CTX_ATTR_KEY);
    ChannelContext channelContext = attribute.get();
    if (channelContext == null) {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      String host = remoteAddress.getAddress().getHostAddress();
      int port = remoteAddress.getPort();
      ChannelContext channelContext1 = ChannelContext.create(IdGenerator.generateId(), Address.create(host, port));

      attribute.set(channelContext1);
      channel.pipeline().fireUserEventTriggered(ChannelSupport.CHANNEL_CTX_CREATED_EVENT);

      channelContextConsumer.accept(channelContext1);
    }
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    Channel channel = ctx.channel();
    channel.config().setAutoRead(false);
    ChannelSupport.closeChannelContextIfExist(ctx.channel());
    super.channelInactive(ctx);
  }
}
