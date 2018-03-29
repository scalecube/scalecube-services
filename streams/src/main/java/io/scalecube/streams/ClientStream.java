package io.scalecube.streams;

import io.scalecube.streams.netty.NettyClientTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CompletableFuture;

public final class ClientStream extends DefaultEventStream {

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

  private NettyClientTransport clientTransport; // calculated

  private ClientStream(Bootstrap bootstrap) {
    clientTransport = new NettyClientTransport(bootstrap, this::subscribe);

    // register cleanup process upfront
    listenClose(aVoid -> clientTransport.close());
  }

  public static ClientStream newClientStream() {
    return new ClientStream(DEFAULT_BOOTSTRAP);
  }

  public static ClientStream newClientStream(Bootstrap bootstrap) {
    return new ClientStream(bootstrap);
  }

  public static Bootstrap getDefaultBootstrap() {
    return DEFAULT_BOOTSTRAP;
  }

  /**
   * Sends a message to a given address.
   * 
   * @param address of target endpoint.
   * @param message to send.
   */
  public void send(Address address, StreamMessage message) {
    CompletableFuture<ChannelContext> connectFuture = clientTransport.getOrConnect(address);
    connectFuture.whenComplete((channelContext, throwable) -> {
      if (channelContext != null) {
        channelContext.postWrite(message);
      }
      if (throwable != null) {
        onNext(Event.writeError(address)
            .message(message)
            .identity("error")
            .error(throwable).build());
      }
    });
  }
}
