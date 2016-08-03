package io.scalecube.transport;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * Catching and logging exceptions.
 * <p/>
 * <b>NOTE:</b> this handler must be the last handler in the pipeline.
 */
@ChannelHandler.Sharable
final class ExceptionCaughtChannelHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(ExceptionCaughtChannelHandler.class);

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.warn("Exception caught for channel {}, {}", ctx.channel(), cause.getMessage(), cause);
  }

}
