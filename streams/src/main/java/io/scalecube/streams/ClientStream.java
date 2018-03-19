package io.scalecube.streams;

import io.scalecube.streams.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class ClientStream extends DefaultEventStream {

  // address instance for clientStreamChannelContext
  public static final Address HELPER_ADDRESS = Address.from("localhost:0");

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

  // special channelContext totally for tracking connect errors on top of ChannelContext<->EventStream framework
  private final ChannelContext clientStreamChannelContext = ChannelContext.create(HELPER_ADDRESS);

  private NettyClientTransport clientTransport; // calculated

  private ClientStream(Bootstrap bootstrap) {
    clientTransport = new NettyClientTransport(bootstrap, this::subscribe);

    // register cleanup process upfront
    listenClose(aVoid -> {
      clientStreamChannelContext.close();
      clientTransport.close();
    });

    // register helper
    subscribe(clientStreamChannelContext);
  }

  public static ClientStream newClientStream() {
    return new ClientStream(DEFAULT_BOOTSTRAP);
  }

  public static ClientStream newClientStream(Bootstrap bootstrap) {
    return new ClientStream(bootstrap);
  }

  /**
   * Sends a message to a given address.
   * 
   * @param address of target endpoint.
   * @param message to send.
   */
  public void send(Address address, StreamMessage message) {
    clientTransport.getOrConnect(address, (channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postWrite(message);
      }
      if (throwable != null) {
        clientStreamChannelContext.postWriteError(address, message, throwable);
      }
    });
  }
}
