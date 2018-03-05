package io.scalecube.ipc.netty;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.transport.Address;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

@Sharable
public final class ChannelContextHandler extends ChannelDuplexHandler {

  private final Consumer<ChannelContext> channelContextConsumer;

  public ChannelContextHandler(Consumer<ChannelContext> channelContextConsumer) {
    this.channelContextConsumer = channelContextConsumer;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    Channel channel = ctx.channel();
    Attribute<ChannelContext> attribute = channel.attr(ChannelSupport.CHANNEL_CTX_ATTR_KEY);
    if (attribute.get() == null) {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      String host = remoteAddress.getAddress().getHostAddress();
      int port = remoteAddress.getPort();
      ChannelContext channelContext = ChannelContext.create(Address.create(host, port));
      attribute.set(channelContext); // set channel attribute

      channelContextConsumer.accept(channelContext);
      channelContext.listenClose(channelContext1 -> {
        if (channel.isActive()) {
          channel.close();
        }
      });

      // fire event to complete channelContext registration
      channel.pipeline().fireUserEventTriggered(ChannelSupport.CHANNEL_CTX_CREATED_EVENT);
    }
    super.channelActive(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    ChannelSupport.closeChannelContextIfExist(ctx);
    super.channelUnregistered(ctx);
  }
}
