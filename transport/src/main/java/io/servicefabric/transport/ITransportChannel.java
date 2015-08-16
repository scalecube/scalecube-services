package io.servicefabric.transport;

import io.servicefabric.transport.protocol.Message;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Represent abstraction over p2p duplex connection between two transport endpoints. Allows sending
 * messages and listen incoming messages.
 */
public interface ITransportChannel {

	/**
	 * Sends message to remote endpoint. Send is async operation, if result of operation is not needed leave second parameter null,
	 * otherwise pass {@link SettableFuture}. If transport channel is already closed - {@code promise} will be failed
	 * with {@link TransportClosedException}.
	 * 
	 * @param message message to send
	 * @param promise promise will be completed with result of sending (void or exception)
	 * @throws IllegalArgumentException if {@code message} is null
	 */
	void send(@CheckForNull Message message, @Nullable SettableFuture<Void> promise);

	/**
	 * Close transport channel, disconnect all available connections which belong to this transport channel.
	 * <br/>
	 * After transport is closed it can't be opened again. New transport channel to the same endpoint can be created.
	 * <br/>
	 * Close is async operation, if result of operation is not needed leave second parameter null,
	 * otherwise pass {@link SettableFuture}. 
	 * 
	 * @param promise promise will be completed with result of closing (void or exception)
	 */
	void close(@Nullable SettableFuture<Void> promise);
}
