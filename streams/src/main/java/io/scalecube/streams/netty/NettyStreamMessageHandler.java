package io.scalecube.streams.netty;

import io.scalecube.streams.ChannelContext;
import io.scalecube.streams.Event;
import io.scalecube.streams.codec.JsonMessageCodec;
import io.scalecube.streams.codec.StreamMessageCodec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.scalecube.streams.codec.TypeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public final class NettyStreamMessageHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyStreamMessageHandler.class);
  private final JsonMessageCodec codec;

  public NettyStreamMessageHandler(TypeResolver codec) {
    this.codec = codec;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object customEvent) throws Exception {
    if (customEvent != ChannelSupport.CHANNEL_CTX_CREATED_EVENT) {
      super.userEventTriggered(ctx, customEvent);
      return;
    }

    ChannelContext channelContext = ChannelSupport.getChannelContextIfExist(ctx);
    if (channelContext == null) {
      LOGGER.error("Can't find channel context on channel: {}", ctx.channel());
      ctx.channel().close();
      return;
    }

    channelContext.listenWrite().map(Event::getMessageOrThrow).subscribe(
        message -> {
          ByteBuf buf = StreamMessageCodec.encode(message);
          ChannelSupport.releaseRefCount(message.data()); // release ByteBuf

          ctx.writeAndFlush(buf).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
              channelContext.postWriteError(message, future.cause());
            } else {
              channelContext.postWriteSuccess(message);
            }
          });
        },
        throwable -> {
          LOGGER.error("Fatal exception occured on channel context: {}, cause: {}", channelContext.getId(), throwable);
          ctx.channel().close();
        });

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
      channelContext.postReadSuccess(StreamMessageCodec.decode((ByteBuf) msg));
    } catch (Exception throwable) {
      ChannelSupport.releaseRefCount(msg);
      channelContext.postReadError(throwable);
    }
  }
}
