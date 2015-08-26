package io.servicefabric.transport;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.utils.ChannelFutureUtils;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inbound handler.
 * Recognizes only handshake message ({@link TransportData#Q_TRANSPORT_HANDSHAKE_SYNC}
 * (rest messages unsupported and results in {@link TransportBrokenException}).
 * Resolves incoming connection metadata with local one (see {@link #resolve(TransportData, Map)}).
 */
@ChannelHandler.Sharable
final class AcceptorHandshakeChannelHandler extends ChannelInboundHandlerAdapter {
	static final Logger LOGGER = LoggerFactory.getLogger(AcceptorHandshakeChannelHandler.class);

	final ITransportSpi transportSpi;

	public AcceptorHandshakeChannelHandler(ITransportSpi transportSpi) {
		this.transportSpi = transportSpi;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Message message = (Message) msg;
		if (!TransportData.Q_TRANSPORT_HANDSHAKE_SYNC.equals(message.header(TransportHeaders.QUALIFIER)))
			throw new TransportBrokenException("Received unsupported " + msg + " (though expecting only Q_TRANSPORT_HANDSHAKE_SYNC)");

		final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		final TransportData handshakeData = (TransportData) message.data();
		final TransportData resolvedData = resolve(handshakeData, transportSpi.getLocalMetadata());
		if (resolvedData.isResolvedOk()) {
			transport.setRemoteHandshake(handshakeData);
			transport.transportSpi.accept(transport);
			transport.transportSpi.resetDueHandshake(transport.channel);
			transport.flip(TransportChannel.Status.CONNECTED, TransportChannel.Status.READY);
			LOGGER.debug("Set READY on acceptor: {}", transport);
		}
		ChannelFuture channelFuture = ctx.writeAndFlush(new Message(resolvedData, TransportHeaders.QUALIFIER, TransportData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK));
		channelFuture.addListener(ChannelFutureUtils.wrap(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				if (!resolvedData.isResolvedOk()) {
					LOGGER.debug("HANDSHAKE({}) not passed, acceptor: {}", resolvedData, transport);
					transport.flip(TransportChannel.Status.CONNECTED, TransportChannel.Status.HANDSHAKE_FAILED,
							new TransportHandshakeException(transport, resolvedData));
					transport.close();
				}
			}
		}));
	}

	/**
	 * Handshake validator method on 'acceptor' side.
	 *
	 * @param handshake     incoming (remote) handshake from 'connector'
	 * @param localMetadata local metadata
	 * @return {@link TransportData} object in status RESOLVED_OK or RESOLVED_ERR
	 */
	private TransportData resolve(TransportData handshake, Map<String, Object> localMetadata) {
		TransportEndpoint originEndpoint = handshake.get(TransportData.META_ORIGIN_ENDPOINT);
		if (originEndpoint == null) {
			return TransportData.ERR(handshake).setExplain(TransportData.META_ORIGIN_ENDPOINT + " not set").build();
		} else if (originEndpoint == localMetadata.get(TransportData.META_ORIGIN_ENDPOINT)) {
			return TransportData.ERR(handshake).setExplain(TransportData.META_ORIGIN_ENDPOINT + " eq to local").build();
		}
		String originEndpointId = handshake.get(TransportData.META_ORIGIN_ENDPOINT_ID);
		if (originEndpointId == null) {
			return TransportData.ERR(handshake).setExplain(TransportData.META_ORIGIN_ENDPOINT_ID + " not set").build();
		} else if (originEndpointId == localMetadata.get(TransportData.META_ORIGIN_ENDPOINT_ID)) {
			return TransportData.ERR(handshake).setExplain(TransportData.META_ORIGIN_ENDPOINT_ID + " eq to local").build();
		}
		return TransportData.OK(localMetadata).build();
	}
}
