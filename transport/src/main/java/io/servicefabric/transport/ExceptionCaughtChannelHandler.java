package io.servicefabric.transport;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catching generic exceptions along with {@link TransportBrokenException}. Catching 'channel inactive' events.
 * <p/>
 * <b>NOTE:</b> this handler must be the last handler in the pipeline.
 */
@ChannelHandler.Sharable
final class ExceptionCaughtChannelHandler extends ChannelDuplexHandler {
	static final Logger LOGGER = LoggerFactory.getLogger(ExceptionCaughtChannelHandler.class);

	final ITransportSpi transportSpi;

	ExceptionCaughtChannelHandler(ITransportSpi transportSpi) {
		this.transportSpi = transportSpi;
	}

	@Override
	public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (cause instanceof TransportBrokenException) {
			TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
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
		TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		LOGGER.debug("Transport inactive: {}", transport);
		transport.close(null/*cause*/, null/*promise*/);
		super.channelInactive(ctx);
	}
}
