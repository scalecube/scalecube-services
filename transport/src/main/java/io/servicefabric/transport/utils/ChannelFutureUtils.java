package io.servicefabric.transport.utils;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import com.google.common.util.concurrent.SettableFuture;

public class ChannelFutureUtils {

	/**
	 * Forward netty's {@code channelFuture} result(success/fail) to guava's {@code promise}.
	 *  
	 * @param channelFuture netty's future
	 * @param promise user passed guava's future (can be null)
	 */
	public static void setPromise(ChannelFuture channelFuture, final SettableFuture<Void> promise) {
		if (promise != null) {
			channelFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						promise.set(future.get());
					} else {
						promise.setException(future.cause());
					}
				}
			});
		}
	}

	/**
	 * @return wrapped {@link ChannelFutureListener} that forwards the Throwable (if any) of given {@code cfl} into the ChannelPipeline. 
	 */
	public static ChannelFutureListener wrap(final ChannelFutureListener cfl) {
		return new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				try {
					cfl.operationComplete(future);
				} catch (Exception e) {
					future.channel().pipeline().fireExceptionCaught(e);
				}
			}
		};
	}
}
