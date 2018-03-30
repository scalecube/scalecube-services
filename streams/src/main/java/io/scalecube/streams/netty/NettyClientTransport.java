package io.scalecube.streams.netty;

import io.scalecube.streams.ChannelContext;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public final class NettyClientTransport {

  private final Bootstrap bootstrap;

  private final ConcurrentMap<Address, Observable<ChannelContext>> outboundChannels = new ConcurrentHashMap<>();

  public NettyClientTransport(Bootstrap bootstrap, Consumer<ChannelContext> channelContextConsumer) {
    Bootstrap bootstrap1 = bootstrap.clone();
    this.bootstrap = bootstrap1.handler(new NettyStreamChannelInitializer(channelContextConsumer));
  }

  /**
   * Async connect to remote address or retrieve existing connection.
   *
   * @param address address to connect to.
   */
  public CompletableFuture<ChannelContext> getOrConnect(Address address) {
    CompletableFuture<ChannelContext> promise = new CompletableFuture<>();
    Observable<ChannelContext> observable = outboundChannels.computeIfAbsent(address, this::connect);
    observable.subscribe(promise::complete, promise::completeExceptionally);
    return promise;
  }

  private Observable<ChannelContext> connect(Address address) {
    AsyncSubject<ChannelContext> subject = AsyncSubject.create();
    ChannelFuture channelFuture = bootstrap.connect(address.host(), address.port());
    channelFuture.addListener((ChannelFutureListener) future -> {
      Throwable error = future.cause(); // nullable
      if (future.isSuccess()) {
        // Hint: this line would activate code in ChannelContextHandler and eventually in NettyStreamMessageHandler;
        // this is done to properly setup netty channel and pin channelContext to it
        future.channel().pipeline().fireChannelActive();
        try {
          ChannelContext channelContext = ChannelSupport.getChannelContextOrThrow(future.channel());
          // register cleanup process upfront
          channelContext.listenClose(input -> outboundChannels.remove(address));
          // emit channel context
          subject.onNext(channelContext);
          subject.onCompleted();
        } catch (Exception throwable) {
          error = throwable;
        }
      }
      if (error != null) {
        outboundChannels.remove(address);
        subject.onError(future.cause());
      }
    });
    Scheduler scheduler = Schedulers.from(channelFuture.channel().eventLoop());
    return subject.subscribeOn(scheduler);
  }

  /**
   * Close all netty channels.
   */
  public void close() {
    // close all channel contexts (there by close all netty channels eventually)
    for (Address address : outboundChannels.keySet()) {
      Observable<ChannelContext> observable = outboundChannels.get(address);
      if (observable != null) {
        observable.subscribe(
            ChannelContext::close,
            throwable -> {
              // Hint: this method is left no-op intentionally
            },
            () -> {
              // Hint: this method is left no-op intentionally
            });
      }
    }
    outboundChannels.clear();
  }
}
