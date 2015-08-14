package io.servicefabric.transport;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.servicefabric.transport.protocol.Message;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

@ChannelHandler.Sharable
final class NetworkEmulatorChannelHandler extends ChannelOutboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulatorChannelHandler.class);

	private final ITransportSpi transportSpi;
	private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings;

	NetworkEmulatorChannelHandler(ITransportSpi transportSpi, Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings) {
		this.transportSpi = transportSpi;
		this.networkSettings = networkSettings;
	}

	@Override
	public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
		Message message = (Message) msg;
		if (TransportData.Q_TRANSPORT_HANDSHAKE_SYNC.equals(message.qualifier()) || TransportData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK.equals(message.qualifier())) {
			super.write(ctx, msg, promise);
			return;
		}
		final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		NetworkEmulatorSettings networkSettings = this.networkSettings.get(transport.getRemoteEndpoint());
		if (networkSettings == null) {
			networkSettings = NetworkEmulatorSettings.defaultSettings();
		}
		if (networkSettings.breakDueToNetwork()) {
			if (promise != null)
				promise.setFailure(new RuntimeException("NETWORK_BREAK detected, not sent " + msg));
			return;
		}
		int timeToSleep = (int) networkSettings.evaluateTimeToSleep();
		if (timeToSleep > 0) {
			try {
				ctx.channel().eventLoop().schedule(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						NetworkEmulatorChannelHandler.super.write(ctx, msg, promise);
						return null;
					}
				}, timeToSleep, TimeUnit.MILLISECONDS);
			} catch (RejectedExecutionException e) {
				if (promise != null) {
					String warn = "Rejected " + msg + " on " + transport;
					LOGGER.warn(warn);
					promise.setFailure(new RuntimeException(warn, e));
				}
			}
			return;
		}
		super.write(ctx, msg, promise);
	}
}
