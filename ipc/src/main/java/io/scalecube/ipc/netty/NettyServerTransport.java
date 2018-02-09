package io.scalecube.ipc.netty;

import static io.scalecube.transport.Addressing.MAX_PORT_NUMBER;
import static io.scalecube.transport.Addressing.MIN_PORT_NUMBER;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ListeningServerStream;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.BindException;
import java.net.InetAddress;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class NettyServerTransport {

  private final ListeningServerStream.Config config;
  private final ServerBootstrap serverBootstrap;

  private Channel serverChannel; // calculated
  private Address address; // calculated

  // Public constructor
  public NettyServerTransport(ListeningServerStream.Config config, Consumer<ChannelContext> channelContextConsumer) {
    this.config = config;
    ServerBootstrap serverBootstrap = config.getServerBootstrap();
    this.serverBootstrap = serverBootstrap.childHandler(new NettyServiceChannelInitializer(channelContextConsumer));
  }

  // Private constructor
  private NettyServerTransport(NettyServerTransport other) {
    this.config = other.config;
    this.serverBootstrap = other.serverBootstrap;
  }

  /**
   * Return server address of the corresponding server channel. If server transport is not yet bound then returned
   * optional object would be empty.
   */
  public Optional<Address> getAddress() {
    return Optional.ofNullable(address);
  }

  /**
   * Async bind server transport listener to address.
   * 
   * @return future when completed bind.
   */
  public CompletableFuture<NettyServerTransport> bind() {
    String listenAddress = config.getListenAddress();
    String listenInterface = config.getListenInterface();
    InetAddress listenAddress1 = Addressing.getLocalIpAddress(listenAddress, listenInterface, config.isPreferIPv6());
    int finalBindPort = config.getPort() + config.getPortCount();
    return new NettyServerTransport(this).bind0(listenAddress1, config.getPort(), finalBindPort);
  }

  /**
   * Async unbind server transport listener from address.
   * 
   * @return future when completed unbind.
   */
  public CompletableFuture<NettyServerTransport> unbind() {
    CompletableFuture<NettyServerTransport> result = new CompletableFuture<>();
    if (serverChannel == null) {
      // Hint: at this point NettyServerTransport isn't bound, but better return instance than exception
      result.complete(NettyServerTransport.this);
      return result;
    }
    serverChannel.close().addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        result.complete(NettyServerTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        result.completeExceptionally(cause);
      }
    });
    return result;
  }

  private CompletableFuture<NettyServerTransport> bind0(InetAddress listenAddress, int bindPort, int finalBindPort) {
    CompletableFuture<NettyServerTransport> result = new CompletableFuture<>();

    // Perform basic bind port validation
    if (bindPort < MIN_PORT_NUMBER || bindPort > MAX_PORT_NUMBER) {
      result.completeExceptionally(
          new IllegalArgumentException("Invalid port number: " + bindPort));
      return result;
    }
    if (bindPort > finalBindPort) {
      result.completeExceptionally(
          new NoSuchElementException("Could not find an available port from: " + bindPort + " to: " + finalBindPort));
      return result;
    }

    // Get address object and bind
    address = Address.create(listenAddress.getHostAddress(), bindPort);

    // Start binding
    ChannelFuture bindFuture = serverBootstrap.bind(listenAddress, address.port());
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        serverChannel = channelFuture.channel();
        result.complete(NettyServerTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
          bind0(listenAddress, bindPort + 1, finalBindPort).thenAccept(result::complete);
        } else {
          result.completeExceptionally(cause);
        }
      }
    });
    return result;
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
  }
}
