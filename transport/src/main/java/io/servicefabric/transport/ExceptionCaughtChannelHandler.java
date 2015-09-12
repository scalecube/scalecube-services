package io.servicefabric.transport;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * Catching generic exceptions along with {@link TransportBrokenException}. Catching 'channel inactive' events.
 * <p/>
 * <b>NOTE:</b> this handler must be the last handler in the pipeline.
 */
@ChannelHandler.Sharable
final class ExceptionCaughtChannelHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(ExceptionCaughtChannelHandler.class);

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof TransportBrokenException) {
      TransportChannel transport = TransportChannel.from(ctx.channel());
      LOGGER.warn("Broken transport: {}, cause: {}", transport, cause);
      transport.close(cause);
    } else if (cause instanceof ClosedChannelException) {
      LOGGER.info("ClosedChannelException caught for channel ", ctx.channel());
    } else if (cause instanceof IOException) {
      LOGGER.info("IOException caught for channel {}, {}", ctx.channel(), cause.getMessage());
    } else {
      LOGGER.error("Exception caught for channel {}, {}", ctx.channel(), cause.getMessage(), cause);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    TransportChannel transport = TransportChannel.from(ctx.channel());
    LOGGER.debug("Transport inactive: {}", transport);
    transport.close();
    super.channelInactive(ctx);
  }
}
