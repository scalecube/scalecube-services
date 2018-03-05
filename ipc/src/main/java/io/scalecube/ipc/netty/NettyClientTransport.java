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

  private final ConcurrentMap<Address, CompletableFuture<ChannelContext>> outboundChannels = new ConcurrentHashMap<>();

  public NettyClientTransport(Bootstrap bootstrap, Consumer<ChannelContext> channelContextConsumer) {
    Bootstrap bootstrap1 = bootstrap.clone();
    this.bootstrap = bootstrap1.handler(new NettyServiceChannelInitializer(channelContextConsumer));
  }

  /**
   * Async connect to remote address or retrieve existing connection.
   * 
   * @param address address to connect to.
   * @return channel context.
   */
  public CompletableFuture<ChannelContext> getOrConnect(Address address) {
    CompletableFuture<ChannelContext> promise = outboundChannels.computeIfAbsent(address, this::connect);
    promise.whenComplete((channelContext, throwable) -> {
      if (throwable != null) { // remove reference right away
        outboundChannels.remove(address, promise);
      }
      if (channelContext != null) { // in case connected subscribe on close event
        channelContext.listenClose(ctx -> outboundChannels.remove(address, promise));
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
          // this line would activate setting of channel ctx attribute
          channel.pipeline().fireChannelActive();
          try {
            // try get channel ctx and complete
            promise.complete(ChannelSupport.getChannelContextOrThrow(channel));
          } catch (Exception throwable) {
            promise.completeExceptionally(throwable);
          }
        });
    return promise;
  }

  /**
   * Disconnect all channels.
   */
  public void close() {
    // close all channels
    for (Address address : outboundChannels.keySet()) {
      CompletableFuture<ChannelContext> promise = outboundChannels.remove(address);
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
