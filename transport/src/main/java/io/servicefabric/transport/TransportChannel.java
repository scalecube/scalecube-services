package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.servicefabric.transport.TransportChannel.Status.CLOSED;
import static io.servicefabric.transport.TransportChannel.Status.CONNECTED;
import static io.servicefabric.transport.TransportChannel.Status.CONNECT_FAILED;
import static io.servicefabric.transport.TransportChannel.Status.CONNECT_IN_PROGRESS;
import static io.servicefabric.transport.TransportChannel.Status.HANDSHAKE_FAILED;
import static io.servicefabric.transport.TransportChannel.Status.HANDSHAKE_IN_PROGRESS;
import static io.servicefabric.transport.TransportChannel.Status.HANDSHAKE_PASSED;
import static io.servicefabric.transport.TransportChannel.Status.NEW;
import static io.servicefabric.transport.utils.ChannelFutureUtils.setPromise;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.servicefabric.transport.protocol.Message;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Func1;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.SettableFuture;

final class TransportChannel implements ITransportChannel {
	static final Logger LOGGER = LoggerFactory.getLogger(TransportChannel.class);

	static final AttributeKey<TransportChannel> ATTR_TRANSPORT = AttributeKey.valueOf("transport");

	enum Status {
		NEW,
		CONNECT_IN_PROGRESS,
		CONNECTED,
		CONNECT_FAILED,
		HANDSHAKE_IN_PROGRESS,
		HANDSHAKE_PASSED,
		HANDSHAKE_FAILED,
		READY,
		CLOSED
	}

	static final EnumSet<Status> failSend = EnumSet.of(CONNECT_FAILED, HANDSHAKE_FAILED, CLOSED);
	static final EnumSet<Status> enqueueSend = EnumSet.of(NEW, CONNECT_IN_PROGRESS, CONNECTED, HANDSHAKE_IN_PROGRESS, HANDSHAKE_PASSED);

	final Channel channel;
	final ITransportSpi transportSpi;
	private final AtomicReference<Status> status = new AtomicReference<>();
	private final AtomicReference<Throwable> cause = new AtomicReference<>();
	private Func1<TransportChannel, Void> whenClose;
	private volatile TransportData remoteHandshake;

	private TransportChannel(Channel channel, ITransportSpi transportSpi) {
		this.channel = channel;
		this.transportSpi = transportSpi;
	}

	/**
	 * Setter for {@link #remoteHandshake}. Called when handshake passed successfully (RESOLVED_OK) on both sides.
	 *
	 * @param remoteHandshake remote handshake (non null)
	 */
	void setRemoteHandshake(TransportData remoteHandshake) {
		checkArgument(remoteHandshake != null);
		this.remoteHandshake = remoteHandshake;
	}

	/**
	 * Origin/Destination of this transport.
	 *
	 * @return TransportEndpoint object this transport is referencing to; or {@code null} if this transport isn't READY yet
	 */
	@Nullable
	public TransportEndpoint getRemoteEndpoint() {
		return remoteHandshake != null ? (TransportEndpoint) remoteHandshake.get(TransportData.META_ORIGIN_ENDPOINT) : null;
	}

	/**
	 * Identity of the Origin/Destination of this transport.
	 *
	 * @return TransportEndpoint object this transport is referencing to; or {@code null} if this transport isn't READY yet
	 */
	@Nullable
	public String getRemoteEndpointId() {
		return remoteHandshake != null ? (String) remoteHandshake.get(TransportData.META_ORIGIN_ENDPOINT_ID) : null;
	}

	@Override
	public void send(@CheckForNull Message message) {
		send(message, null);
	}

	@Override
	public void send(@CheckForNull Message message, @Nullable SettableFuture<Void> promise) {
		checkArgument(message != null);
		if (shouldFailSend()) {
			if (promise != null)
				promise.setException(cause.get());
		} else {
			setPromise(channel.writeAndFlush(message), promise);
		}
	}

	boolean shouldFailSend() {
		return failSend.contains(status.get());
	}

	boolean shouldEnqueueSend() {
		return enqueueSend.contains(status.get());
	}

	@Override
	public void close(@Nullable SettableFuture<Void> promise) {
		close(null/*cause*/, promise);
	}

	void close(Throwable cause) {
		close(cause, null/*promise*/);
	}

	void close(Throwable cause, SettableFuture<Void> promise) {
		status.set(CLOSED);
		this.cause.compareAndSet(null, cause != null ? cause : new TransportClosedException(this));
		whenClose.call(this);
		setPromise(channel.close(), promise);
		LOGGER.debug("Closed {}", this);
	}

	/**
	 * Flips the {@link #status}.
	 *
	 * @throws TransportBrokenException in case {@code expect} not actual
	 */
	void flip(Status expect, Status update) throws TransportBrokenException {
		if (!status.compareAndSet(expect, update)) {
			String err = "Can't set status " + update + " (expect=" + expect + ", actual=" + status + ")";
			throw new TransportBrokenException(this, err);
		}
	}

	Throwable getCause() {
		return cause.get();
	}

	@Override
	public String toString() {
		if (getCause() == null) {
			return "NettyTransport{" +
					"status=" + status +
					", channel=" + channel +
					'}';
		}
		Class clazz = getCause().getClass();
		String packageName = clazz.getPackage().getName();
		String dottedPackageName = Joiner.on('.').join(transform(Splitter.on('.').split(packageName),
				new Function<String, Character>() {
					@Override
					public Character apply(String input) {
						return input.charAt(0);
					}
				}));
		return "NettyTransport{" +
				"status=" + status +
				", cause=[" + dottedPackageName + "." + clazz.getSimpleName() + "]" +
				", channel=" + channel +
				'}';
	}

	final static class Builder {
		private TransportChannel target;

		static Builder CONNECTOR(Channel channel, ITransportSpi transportSpi) {
			Builder builder = new Builder();
			builder.target = new TransportChannel(channel, transportSpi);
			builder.target.status.set(NEW);
			return builder;
		}

		static Builder ACCEPTOR(Channel channel, ITransportSpi transportSpi) {
			Builder builder = new Builder();
			builder.target = new TransportChannel(channel, transportSpi);
			builder.target.status.set(CONNECTED);
			return builder;
		}

		Builder set(Func1<TransportChannel, Void> f) {
			target.whenClose = f;
			return this;
		}

		TransportChannel build() {
			return target;
		}
	}
}
