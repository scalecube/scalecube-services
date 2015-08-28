package io.servicefabric.transport;

import static com.google.common.base.Throwables.propagate;
import static io.servicefabric.transport.utils.ChannelFutureUtils.wrap;
import static java.lang.Thread.interrupted;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ScheduledFuture;
import io.servicefabric.transport.protocol.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Duplex handler.
 * On inbound recognizes only handshake message {@link TransportData#Q_TRANSPORT_HANDSHAKE_SYNC_ACK}
 * (rest inbound messages unsupported and results in {@link TransportBrokenException}).
 * On outbound may enqueue messages (see {@link TransportChannel#shouldEnqueueSend()}).
 * On 'channel active' starting handshake process.
 * <p/>
 * <b>NOTE:</b> this handler is not shareable (see {@link #sendMailbox}, {@link #handshakeTimer}); 
 * and should always run in different executor than io-thread.
 */
final class ConnectorHandshakeChannelHandler extends ChannelDuplexHandler {
	static final Logger LOGGER = LoggerFactory.getLogger(ConnectorHandshakeChannelHandler.class);

	final ITransportSpi transportSpi;
	BlockingQueue<WriteAndFlush> sendMailbox;
	ScheduledFuture handshakeTimer;

	/** Tuple class. Contains message and promise. */
	static class WriteAndFlush {
		final Object msg;
		final ChannelPromise promise;

		WriteAndFlush(Object msg, ChannelPromise promise) {
			this.msg = msg;
			this.promise = promise;
		}
	}

	ConnectorHandshakeChannelHandler(ITransportSpi transportSpi) {
		this.transportSpi = transportSpi;
		this.sendMailbox = new ArrayBlockingQueue<>(transportSpi.getSendHwm(), true/*fair*/);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		transport.flip(TransportChannel.Status.CONNECT_IN_PROGRESS, TransportChannel.Status.CONNECTED);

		final TransportData handshake = TransportData.NEW(transport.transportSpi.getLocalMetadata()).build();
		int handshakeTimeout = transport.transportSpi.getHandshakeTimeout();
		handshakeTimer = transport.channel.eventLoop().schedule(new Runnable() {
			@Override
			public void run() {
				LOGGER.debug("HANDSHAKE_SYNC({}) timeout, connector: {}", handshake, transport);
				transport.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_FAILED,
						new TransportHandshakeException(transport, handshake, new TimeoutException()));
				transport.close();
			}
		}, handshakeTimeout, TimeUnit.MILLISECONDS);

		transport.flip(TransportChannel.Status.CONNECTED, TransportChannel.Status.HANDSHAKE_IN_PROGRESS);

		ChannelFuture channelFuture = ctx.writeAndFlush(new Message(handshake, TransportHeaders.QUALIFIER, TransportData.Q_TRANSPORT_HANDSHAKE_SYNC));
		channelFuture.addListener(wrap(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				if (!future.isSuccess()) {
					LOGGER.debug("HANDSHAKE_SYNC({}) not sent, connector: {}", handshake, transport);
					cancelHandshakeTimer();
					transport.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_FAILED,
							new TransportHandshakeException(transport, handshake, future.cause()));
					transport.close();
				}
			}
		}));

		super.channelActive(ctx);
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
		TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		if (transport.shouldEnqueueSend()) {
			try {
				sendMailbox.put(new WriteAndFlush(msg, promise));
			} catch (InterruptedException e) {
				interrupted();
				if (promise != null)
					promise.setFailure(e);
				propagate(e);
			}
		} else {
			ctx.writeAndFlush(msg, promise);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		Message message = (Message) msg;
		if (!TransportData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK.equals(message.header(TransportHeaders.QUALIFIER)))
			throw new TransportBrokenException("Received unsupported " + msg + " (though expecting only Q_TRANSPORT_HANDSHAKE_SYNC_ACK)");

		TransportData handshake = (TransportData) message.data();
		final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		if (handshake.isResolvedOk()) {
			cancelHandshakeTimer();
			transport.setRemoteHandshake(handshake);
			transport.transportSpi.resetDueHandshake(transport.channel);
			transport.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_PASSED);
			LOGGER.debug("HANDSHAKE passed on connector: {}", transport);
			drainSendMailbox(ctx);
			transport.flip(TransportChannel.Status.HANDSHAKE_PASSED, TransportChannel.Status.READY);
			LOGGER.debug("Set READY on connector: {}", transport);
		} else {
			LOGGER.debug("HANDSHAKE({}) not passed, connector: {}", handshake, transport);
			cancelHandshakeTimer();
			transport.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_FAILED,
					new TransportHandshakeException(transport, handshake));
			transport.close();
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		cancelHandshakeTimer();
		cleanupSendMailbox(ctx);
		super.channelInactive(ctx);
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
		cancelHandshakeTimer();
		cleanupSendMailbox(ctx);
		super.close(ctx, future);
	}

	private void drainSendMailbox(ChannelHandlerContext ctx) {
		while (!sendMailbox.isEmpty()) {
			WriteAndFlush waf = sendMailbox.poll();
			ctx.writeAndFlush(waf.msg, waf.promise);
		}
	}

	private void cleanupSendMailbox(ChannelHandlerContext ctx) {
		TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
		Throwable transportCause = transport.getCause();
		Throwable cause = transportCause != null ? transportCause : new TransportClosedException(transport);
		while (!sendMailbox.isEmpty()) {
			WriteAndFlush waf = sendMailbox.poll();
			if (waf.promise != null) {
				waf.promise.setFailure(cause);
			}
		}
	}

	private void cancelHandshakeTimer() {
		if (handshakeTimer != null) {
			handshakeTimer.cancel(true);
		}
	}
}
