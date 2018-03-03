package io.scalecube.ipc;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Emitter;
import rx.Observable;

public final class SubscriberStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberStream.class);

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

  private final ServerStream serverStream;
  private final ClientStream clientStream;

  //// Constructors

  private SubscriberStream(SubscriberStream other) {
    this(other.serverStream, other.clientStream);
  }

  private SubscriberStream(ServerStream serverStream, ClientStream clientStream) {
    this.serverStream = serverStream;
    this.clientStream = clientStream;
  }

  /**
   * Factory method. Creates new SubscriberStream with default bootstrap.
   */
  public static SubscriberStream newSubscriberStream() {
    return newSubscriberStream(DEFAULT_BOOTSTRAP);
  }

  /**
   * Factory method. Creates new SubscriberStream with custom {@link Bootstrap}.
   */
  public static SubscriberStream newSubscriberStream(Bootstrap bootstrap) {
    ServerStream serverStream = ServerStream.newServerStream();
    ClientStream clientStream = ClientStream.newClientStream(bootstrap);

    // request logic
    serverStream.listen()
        .filter(Event::isMessageWrite)
        .subscribe(event -> clientStream.send(event.getAddress(), event.getMessage().get()));

    // response logic
    clientStream.listenMessageReadSuccess().subscribe(
        message -> serverStream.send(message,
            (identity, message1) -> ChannelContext.getIfExist(identity).postReadSuccess(message1),
            throwable -> LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable)));

    return new SubscriberStream(serverStream, clientStream);
  }

  /**
   * Sends a message to a given address and listens response traffic on returned subscription point. Internally creates
   * new instance of channelContext (a new 'subscriber') attached to serverStream<->clientStream communication, and
   * returns subscription point back to caller.
   *
   * @param address of target endpoint.
   * @param message to send, a request.
   * @return subscription point for receiving response messages from remote party.
   */
  public Observable<ServiceMessage> listenOnNext(Address address, ServiceMessage message) {
    return Observable.create(emitter -> {
      ChannelContext channelContext;
      try {
        // create 'subscriber'
        serverStream.subscribe(channelContext = ChannelContext.create(address));
        // register cleanup process upfront
        emitter.setCancellation(channelContext::close);
        // subscribe and emit request (in this order)
        channelContext.listenMessageReadSuccess().subscribe(emitter);
        channelContext.postMessageWrite(message);
      } catch (Exception throwable) {
        emitter.onError(throwable);
      }
    }, Emitter.BackpressureMode.BUFFER);
  }

  /**
   * Closes shared (across {@link SubscriberStream} instances) serverStream and clientStream. After this call this
   * instance wouldn't emit events on subsequent {@link #listenOnNext(Address, ServiceMessage)}.
   */
  public void close() {
    serverStream.close();
    clientStream.close();
  }
}
