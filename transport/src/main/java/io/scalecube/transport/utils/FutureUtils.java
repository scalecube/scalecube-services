package io.scalecube.transport.utils;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import javax.annotation.Nullable;

public class FutureUtils {

  private FutureUtils() {}

  /**
   * Converts netty {@link Future} to the given guava {@link ListenableFuture}.
   * 
   * @param future netty future
   * @param promise guava future; can be null
   * @return same {@code promise} passed as parameter; just for convenience
   */
  public static <T> ListenableFuture<T> compose(Future<T> future, @Nullable final SettableFuture<T> promise) {
    if (promise != null) {
      future.addListener(new GenericFutureListener<Future<T>>() {
        @Override
        public void operationComplete(Future<T> future) throws Exception {
          if (future.isSuccess()) {
            promise.set(future.get());
          } else {
            promise.setException(future.cause());
          }
        }
      });
    }
    return promise;
  }

  /**
   * Converts netty {@link Future} to the newly created guava {@link ListenableFuture}.
   *
   * @param future netty future
   * @return newly created {@code listenableFuture}
   */
  public static <T> ListenableFuture<T> compose(Future<T> future) {
    final SettableFuture<T> promise = SettableFuture.create();
    future.addListener(new GenericFutureListener<Future<T>>() {
      @Override
      public void operationComplete(Future<T> future) throws Exception {
        if (future.isSuccess()) {
          promise.set(future.get());
        } else {
          promise.setException(future.cause());
        }
      }
    });
    return promise;
  }

  /**
   * @return wrapped {@link ChannelFutureListener} that forwards a throwable thrown out of given
   *         {@code channelFutureListener} into the {@code ChannelPipeline}.
   */
  public static ChannelFutureListener wrap(final ChannelFutureListener channelFutureListener) {
    return new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        try {
          channelFutureListener.operationComplete(future);
        } catch (Exception e) {
          future.channel().pipeline().fireExceptionCaught(e);
        }
      }
    };
  }
}
