package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class ClientStream implements EventStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientStream.class);

  private static final Bootstrap DEFAULT_BOOTSTRAP;
  // Pre-configure default bootstrap
  static {
    DEFAULT_BOOTSTRAP = new Bootstrap()
        .group(new NioEventLoopGroup(0))
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true);
  }

  private final Subject<Event, Event> subject = PublishSubject.<Event>create().toSerialized();

  private NettyClientTransport clientTransport; // calculated

  private ClientStream(Bootstrap bootstrap) {
    clientTransport = new NettyClientTransport(bootstrap, channelContext -> subscribe(channelContext.listen(),
        cause -> LOGGER.error("Unsubscribed client {} due to unhandled exception caught: {}", channelContext, cause),
        aVoid -> LOGGER.debug("Unsubscribed client {} due to completion", channelContext)));
    // register cleanup process upfront
    listen().subscribe(event -> {
    }, throwable -> clientTransport.close(), clientTransport::close);
  }

  public static ClientStream newClientStream() {
    return new ClientStream(DEFAULT_BOOTSTRAP);
  }

  public static ClientStream newClientStream(Bootstrap bootstrap) {
    return new ClientStream(bootstrap);
  }

  @Override
  public void subscribe(Observable<Event> observable, Consumer<Throwable> onError, Consumer<Void> onCompleted) {
    observable.subscribe(subject::onNext, onError::accept, () -> onCompleted.accept(null));
  }

  @Override
  public void close() {
    subject.onCompleted();
  }

  /**
   * Sends a message to a given address.
   * 
   * @param address of target endpoint.
   * @param message to send.
   */
  public void send(Address address, ServiceMessage message) {
    CompletableFuture<ChannelContext> promise = clientTransport.getOrConnect(address);
    promise.whenComplete((channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postMessageWrite(message);
      }
    });
  }

  /**
   * Subscription point for events coming from client channels.
   */
  @Override
  public Observable<Event> listen() {
    return subject.onBackpressureBuffer().asObservable();
  }
}
