package io.scalecube.ipc;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

public final class TransportStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportStream.class);

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

  private ChannelContext channelContext; // calculated

  //// Constructors

  private TransportStream(TransportStream other) {
    this(other.serverStream, other.clientStream);
  }

  private TransportStream(ServerStream serverStream, ClientStream clientStream) {
    this.serverStream = serverStream;
    this.clientStream = clientStream;
  }

  public static TransportStream newTransportStream() {
    return newTransportStream(DEFAULT_BOOTSTRAP);
  }

  public static TransportStream newTransportStream(Bootstrap bootstrap) {
    ServerStream serverStream = ServerStream.newServerStream();
    ClientStream clientStream = ClientStream.newClientStream(bootstrap);

    // request logic
    serverStream.listen()
        .filter(Event::isMessageWrite)
        .subscribe(event -> clientStream.send(event.getAddress(), event.getMessage().get()));

    // response logic
    clientStream.listen()
        .filter(Event::isReadSuccess)
        .map(event -> event.getMessage().get())
        .subscribe(message -> serverStream.send(message,
            (identity, message1) -> ChannelContext.getIfExist(identity).postReadSuccess(message1),
            throwable -> LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable)));

    return new TransportStream(serverStream, clientStream);
  }

  /**
   * Sends a message to a given address. After calling this method it becomes eligible to subscribe on {@link #listen()}
   * to receive messages from remote party.
   *
   * @param address of target endpoint.
   * @param message to send.
   * @return TransportStream instance with dedicated channelContext attached to serverStream to clientStream (and
   *         opposite direction as well) communication.
   */
  public TransportStream send(Address address, ServiceMessage message) {
    TransportStream exchangeStream = new TransportStream(this);

    // create new 'exchange point' and subscribe serverStream on it
    exchangeStream.channelContext = ChannelContext.create(IdGenerator.generateId(), address);
    exchangeStream.serverStream.subscribe(exchangeStream.channelContext);

    // emit message write request, there by activate serverStream
    exchangeStream.channelContext.postMessageWrite(message);

    return exchangeStream;
  }

  /**
   * This is subscription point method after calling {@link #send(Address, ServiceMessage)}. NOTE: calling it with out
   * corresponding send will result in IllegalStateException.
   */
  public Observable<Event> listen() {
    if (channelContext == null) {
      Observable.error(new IllegalStateException("Call send() first"));
    }
    return channelContext.listenReadSuccess();
  }

  /**
   * Closes shared (across {@link TransportStream} instances) serverStream and clientStream. After this call this
   * instance wouldn't emit events neither on further {@link #send(Address, ServiceMessage)} call neither on
   * corresponding {@link #listen()} call.
   */
  public void destroy() {
    serverStream.close();
    clientStream.close();
    close();
  }

  /**
   * Closes channelContext (if any) that was created at corresponding {@link #send(Address, ServiceMessage)} call.
   */
  public void close() {
    if (channelContext != null) {
      channelContext.close();
    }
  }
}
