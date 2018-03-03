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

public final class ExchangeStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeStream.class);

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

  private ExchangeStream(ExchangeStream other) {
    this(other.serverStream, other.clientStream);
  }

  private ExchangeStream(ServerStream serverStream, ClientStream clientStream) {
    this.serverStream = serverStream;
    this.clientStream = clientStream;
  }

  /**
   * Creates new ExchangeStream with default bootstrap.
   */
  public static ExchangeStream newExchangeStream() {
    return newExchangeStream(DEFAULT_BOOTSTRAP);
  }

  /**
   * Creates new ExchangeStream with custom {@link Bootstrap}.
   */
  public static ExchangeStream newExchangeStream(Bootstrap bootstrap) {
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

    return new ExchangeStream(serverStream, clientStream);
  }

  /**
   * Sends a message to a given address. Internally creates new instance of channelContext (a new 'exchange point')
   * attached to serverStream<->clientStream communication, and returns subscription point back to caller.
   *
   * @param address of target endpoint.
   * @param message to send.
   * @return subscription point for receiving messages from remote party.
   */
  public Observable<ServiceMessage> send(Address address, ServiceMessage message) {
    ChannelContext channelContext = ChannelContext.create(address);
    return Observable.create(emitter -> {
      serverStream.subscribe(channelContext);
      // subscribe
      channelContext.listenMessageReadSuccess().subscribe(emitter);
      // emit request
      channelContext.postMessageWrite(message);
    }, Emitter.BackpressureMode.BUFFER);
  }

  /**
   * Closes shared (across {@link ExchangeStream} instances) serverStream and clientStream. After this call this
   * instance wouldn't emit events on subsequent {@link #send(Address, ServiceMessage)}.
   */
  public void close() {
    serverStream.close();
    clientStream.close();
  }
}
