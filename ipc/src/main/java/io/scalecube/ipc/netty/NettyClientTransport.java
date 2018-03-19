package io.scalecube.ipc.netty;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class NettyClientTransport {

  private final Bootstrap bootstrap;

  private final ConcurrentMap<Address, Observable<ChannelContext>> outboundChannels = new ConcurrentHashMap<>();

  public NettyClientTransport(Bootstrap bootstrap, Consumer<ChannelContext> channelContextConsumer) {
    Bootstrap bootstrap1 = bootstrap.clone();
    this.bootstrap = bootstrap1.handler(new NettyServiceChannelInitializer(channelContextConsumer));
  }

  /**
   * Async connect to remote address or retrieve existing connection.
   * 
   * @param address address to connect to.
   * @param consumer biConsumer with optional channelContext and optional Throwable.
   */
  public void getOrConnect(Address address, BiConsumer<ChannelContext, Throwable> consumer) {
    Observable<ChannelContext> observable = outboundChannels.computeIfAbsent(address, this::connect);
    observable.subscribe(
        channelContext -> { // register cleanup process upfront
          channelContext.listenClose(input -> outboundChannels.remove(address, observable));
          consumer.accept(channelContext, null);
        },
        throwable -> { // clean map right away
          outboundChannels.remove(address, observable);
          consumer.accept(null, throwable);
        },
        () -> { // no-op
        });
  }

  private Observable<ChannelContext> connect(Address address) {
    BehaviorSubject<ChannelContext> subject = BehaviorSubject.create();

    ChannelFuture connectFuture = bootstrap.connect(address.host(), address.port());
    connectFuture.addListener((ChannelFutureListener) channelFuture -> {
      Channel channel = channelFuture.channel();
      if (!channelFuture.isSuccess()) {
        subject.onError(channelFuture.cause());
        return;
      }
      // this line pins channelContext to netty channel
      channel.pipeline().fireChannelActive();
      try {
        // try get channelContext and complete
        subject.onNext(ChannelSupport.getChannelContextOrThrow(channel));
        subject.onCompleted();
      } catch (Exception throwable) {
        subject.onError(throwable);
      }
    });

    return subject;
  }

  /**
   * Disconnect all channels.
   */
  public void close() {
    // close all channels
    for (Address address : outboundChannels.keySet()) {
      Observable<ChannelContext> observable = outboundChannels.remove(address);
      if (observable != null) {
        observable.subscribe(ChannelContext::close);
      }
    }
  }
}
