package io.servicefabric.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.servicefabric.transport.protocol.Message;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Channel handler for getting message traffic.
 * Activated when connection established/accepted and handshake passed.
 * <p/>
 * <b>NOTE:</b> in the pipeline this handler must be set just right before {@link ExceptionCaughtChannelHandler}.
 */
@ChannelHandler.Sharable
final class MessageReceiverChannelHandler extends ChannelInboundHandlerAdapter {
	static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiverChannelHandler.class);

	final ITransportSpi transportSpi;

	MessageReceiverChannelHandler(ITransportSpi transportSpi) {
		this.transportSpi = transportSpi;
	}

	/** Publish {@code msg} on the TransportFactory subject. */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		TransportMessage transportMessage = new TransportMessage(transport,
				(Message) msg,
				transport.getRemoteEndpoint(),
				transport.getRemoteEndpointId());
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Received {} from {}", transportMessage.message(), transportMessage.originEndpoint());
		}
		transportSpi.getSubject().onNext(transportMessage);
	}
}
