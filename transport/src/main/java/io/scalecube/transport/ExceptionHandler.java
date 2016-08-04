package io.scalecube.transport;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catching and logging exceptions.
 * <p/>
 * <b>NOTE:</b> this handler must be the last handler in the pipeline.
 */
@ChannelHandler.Sharable
final class ExceptionHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.warn("Exception caught for channel {}, {}", ctx.channel(), cause.getMessage(), cause);
  }

}
