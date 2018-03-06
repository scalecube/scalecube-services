package io.scalecube.ipc;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamProcessorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessorFactory.class);

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

  private StreamProcessorFactory(ServerStream serverStream, ClientStream clientStream) {
    this.serverStream = serverStream;
    this.clientStream = clientStream;
  }

  /**
   * Factory method. Creates new {@link StreamProcessorFactory} with default bootstrap.
   */
  public static StreamProcessorFactory newStreamProcessorFactory() {
    return newStreamProcessorFactory(DEFAULT_BOOTSTRAP);
  }

  /**
   * Factory method. Creates new {@link StreamProcessorFactory} with custom {@link Bootstrap}.
   */
  public static StreamProcessorFactory newStreamProcessorFactory(Bootstrap bootstrap) {
    ServerStream serverStream = ServerStream.newServerStream();
    ClientStream clientStream = ClientStream.newClientStream(bootstrap);

    // request logic
    serverStream.listen()
        .filter(Event::isMessageWrite)
        .subscribe(event -> clientStream.send(event.getAddress(), event.getMessageOrThrow()));

    // response logic
    clientStream.listenMessageReadSuccess().subscribe(
        message -> serverStream.send(message,
            (identity, message1) -> ChannelContext.getIfExist(identity).postReadSuccess(message1),
            throwable -> LOGGER.warn("Failed to handle message: {}, cause: {}", message, throwable)));

    // send failed to remote party => do cleanup
    clientStream.listen().filter(Event::isWriteError).subscribe(event -> {
      // serverStream::unsubscribe
    });

    // remote party gone => do cleanup
    clientStream.listenChannelContextClosed().map(Event::getAddress).subscribe(serverStream::unsubscribe);

    return new StreamProcessorFactory(serverStream, clientStream);
  }

  /**
   * @param address target address for processor.
   * @return new {@link StreamProcessor} object with reference to this serverStream and given address.
   */
  public StreamProcessor create(Address address) {
    return new StreamProcessor(address, serverStream);
  }

  /**
   * Closes shared (across {@link StreamProcessor} instances) serverStream and clientStream.
   */
  public void close() {
    serverStream.close();
    clientStream.close();
  }
}
