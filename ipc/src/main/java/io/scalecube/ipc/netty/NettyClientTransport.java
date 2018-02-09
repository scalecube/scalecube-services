package io.scalecube.ipc.netty;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public final class NettyClientTransport {

  private final Bootstrap bootstrap;

  private final ConcurrentMap<Address, CompletableFuture<ChannelContext>> outgoingChannels = new ConcurrentHashMap<>();

  public NettyClientTransport(Bootstrap bootstrap, Consumer<ChannelContext> channelContextConsumer) {
    this.bootstrap = bootstrap.handler(new NettyServiceChannelInitializer(channelContextConsumer));
  }

  /**
   * a-sync connect to remote address or retrieve existing connection.
   * 
   * @param address address to connect to.
   * @return channel context.
   */
  public CompletableFuture<ChannelContext> getOrConnect(Address address) {
    CompletableFuture<ChannelContext> promise = outgoingChannels.computeIfAbsent(address, this::connect);
    promise.whenComplete((channelContext, throwable) -> {
      if (throwable != null) {
        outgoingChannels.remove(address, promise);
      }
      if (channelContext != null) {
        channelContext.listenClose().subscribe(aVoid -> outgoingChannels.remove(address, promise));
      }
    });
    return promise;
  }

  private CompletableFuture<ChannelContext> connect(Address address) {
    CompletableFuture<ChannelContext> promise = new CompletableFuture<>();
    bootstrap.connect(address.host(), address.port())
        .addListener((ChannelFutureListener) channelFuture -> {
          Channel channel = channelFuture.channel();
          if (!channelFuture.isSuccess()) {
            promise.completeExceptionally(channelFuture.cause());
            return;
          }
          channel.pipeline().fireChannelActive();
          try {
            promise.complete(ChannelSupport.getChannelContextOrThrow(channel));
          } catch (Exception e) {
            promise.completeExceptionally(e);
          }
        });
    return promise;
  }

  /**
   * disconnect all channels.
   */
  public void close() {
    // close all channels
    for (Address address : outgoingChannels.keySet()) {
      CompletableFuture<ChannelContext> promise = outgoingChannels.remove(address);
      if (promise != null) {
        promise.whenComplete((channelContext, throwable) -> {
          if (channelContext != null) {
            channelContext.close();
          }
        });
      }
    }
  }
}
