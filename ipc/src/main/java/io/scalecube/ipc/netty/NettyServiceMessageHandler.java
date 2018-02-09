package io.scalecube.ipc.netty;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ServiceMessage;
import io.scalecube.ipc.codec.ServiceMessageCodec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public final class NettyServiceMessageHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyServiceMessageHandler.class);

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object customEvent) throws Exception {
    if (customEvent == ChannelSupport.CHANNEL_CTX_CREATED_EVENT) {

      ChannelContext channelContext = ChannelSupport.getChannelContextIfExist(ctx);
      if (channelContext == null) {
        LOGGER.error("Can't find channel context on channel: {}", ctx.channel());
        ctx.channel().close();
        return;
      }

      channelContext.listenMessageWrite().subscribe(
          event -> {
            ServiceMessage message = event.getMessage().get();
            ByteBuf buf = ServiceMessageCodec.encode(message);
            ChannelSupport.releaseRefCount(message.getData()); // release ByteBuf

            ctx.writeAndFlush(buf).addListener((ChannelFutureListener) future -> {
              if (!future.isSuccess()) {
                channelContext.postWriteError(future.cause(), message);
              } else {
                channelContext.postWriteSuccess(message);
              }
            });
          },
          throwable -> {
            LOGGER.error("Fatal exception occured on channel context: {}, cause: {}", channelContext.getId(),
                throwable);
            ctx.channel().close();
          });
    }

    super.userEventTriggered(ctx, customEvent);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ChannelContext channelContext = ChannelSupport.getChannelContextIfExist(ctx);
    if (channelContext == null) {
      LOGGER.error("Failed to handle message, channel context is null on channel: {}", ctx.channel());
      ChannelSupport.releaseRefCount(msg);
      ctx.channel().close();
      return;
    }

    try {
      channelContext.postReadSuccess(ServiceMessageCodec.decode((ByteBuf) msg));
    } catch (Exception throwable) {
      ChannelSupport.releaseRefCount(msg);
      channelContext.postReadError(throwable);
    }
  }
}
