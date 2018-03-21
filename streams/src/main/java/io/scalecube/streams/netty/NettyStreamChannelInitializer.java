package io.scalecube.streams.netty;

import io.scalecube.streams.ChannelContext;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

@Sharable
public final class NettyStreamChannelInitializer extends ChannelInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyStreamChannelInitializer.class);

  private static final int LENGTH_FIELD_LENGTH = 4;
  private static final int MAX_FRAME_LENGTH = 2048000;

  private final ChannelContextHandler channelContextHandler;
  private final NettyStreamMessageHandler messageHandler;

  public NettyStreamChannelInitializer(Consumer<ChannelContext> channelContextConsumer) {
    this.channelContextHandler = new ChannelContextHandler(channelContextConsumer);
    this.messageHandler = new NettyStreamMessageHandler();
  }

  @Override
  protected void initChannel(Channel channel) {
    ChannelPipeline pipeline = channel.pipeline();
    // contexs contexts contexs
    channel.pipeline().addLast(channelContextHandler);
    // frame codecs
    pipeline.addLast(new LengthFieldPrepender(LENGTH_FIELD_LENGTH));
    pipeline
        .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, LENGTH_FIELD_LENGTH, 0, LENGTH_FIELD_LENGTH));
    // message acceptor
    pipeline.addLast(messageHandler);
    // at-least-something exception handler
    pipeline.addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
        // Hint: at this point one can look at throwable, make some exception translation, and via channelContext post
        // ChannelContextError event, and hence give business layer ability to react on low level system error events
        LOGGER.warn("Exception caught for channel {}, {}", ctx.channel(), throwable);
      }
    });
  }
}
