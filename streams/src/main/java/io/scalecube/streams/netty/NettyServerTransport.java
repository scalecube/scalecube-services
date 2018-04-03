package io.scalecube.streams.netty;

import static io.scalecube.transport.Addressing.MAX_PORT_NUMBER;
import static io.scalecube.transport.Addressing.MIN_PORT_NUMBER;

import io.scalecube.streams.ChannelContext;
import io.scalecube.streams.ListeningServerStream;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ServerChannel;

import java.net.BindException;
import java.net.InetAddress;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public final class NettyServerTransport {

  private final ListeningServerStream.Config config;
  private final ServerBootstrap serverBootstrap;

  private final ConcurrentMap<Address, ChannelContext> inboundChannels = new ConcurrentHashMap<>();

  private Address serverAddress; // calculated
  private ServerChannel serverChannel; // calculated

  /**
   * Public constructor for this transport. {@link ListeningServerStream} call this constructor passing his config
   * object and channelContext logic handler as second argument.
   *
   * @param config from {@link ListeningServerStream} object.
   * @param channelContextConsumer logic provider around connected {@link ChannelContext}.
   */
  public NettyServerTransport(ListeningServerStream.Config config, Consumer<ChannelContext> channelContextConsumer) {
    this.config = config;

    Consumer<ChannelContext> consumer = channelContextConsumer.andThen(channelContext -> {
      // register cleanup process upfront
      channelContext.listenClose(input -> inboundChannels.remove(input.getAddress(), input));
      // save accepted channel
      inboundChannels.put(channelContext.getAddress(), channelContext);
    });

    ServerBootstrap bootstrap = config.getServerBootstrap().clone();
    this.serverBootstrap = bootstrap.childHandler(new NettyStreamChannelInitializer(consumer));
  }

  /**
   * Return bound address of the corresponding server channel. If server transport is not yet bound then returned
   * optional object would be empty.
   */
  public Optional<Address> getServerAddress() {
    return Optional.ofNullable(serverAddress);
  }

  /**
   * Async bind server transport listener to address.
   *
   * @return future when completed bind.
   */
  public CompletableFuture<NettyServerTransport> bind() {
    String listenAddress = config.getListenAddress();
    String listenInterface = config.getListenInterface();
    boolean preferIPv6 = config.isPreferIPv6();
    int port = config.getPort();
    int finalPort = port + config.getPortCount();
    InetAddress bindAddress = Addressing.getLocalIpAddress(listenAddress, listenInterface, preferIPv6);
    return bind0(bindAddress, port, finalPort);
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
    } else {
      serverChannel.close().addListener((ChannelFutureListener) channelFuture -> {
        if (channelFuture.isSuccess()) {
          result.complete(NettyServerTransport.this);
        } else {
          result.completeExceptionally(channelFuture.cause());
        }
      });
    }
    return result;
  }

  private CompletableFuture<NettyServerTransport> bind0(InetAddress bindAddress, int bindPort, int finalBindPort) {
    CompletableFuture<NettyServerTransport> result = new CompletableFuture<>();

    // Perform basic bind port validation
    if (bindPort < MIN_PORT_NUMBER || bindPort > MAX_PORT_NUMBER) {
      result.completeExceptionally(
          new IllegalArgumentException("Invalid port number: " + bindPort));
      return result;
    }
    if (bindPort > finalBindPort) {
      result.completeExceptionally(
          new NoSuchElementException("Could not find an available port from: "
              + bindPort + " to: " + finalBindPort));
      return result;
    }

    // Start binding
    ChannelFuture bindFuture = serverBootstrap.bind(bindAddress, bindPort);
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        // init fields
        NettyServerTransport.this.init(bindAddress, bindPort, (ServerChannel) channelFuture.channel());
        // complete
        result.complete(NettyServerTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
          bind0(bindAddress, bindPort + 1, finalBindPort).thenAccept(result::complete);
        } else {
          result.completeExceptionally(cause);
        }
      }
    });
    return result;
  }

  private void init(InetAddress address, int port, ServerChannel serverChannel) {
    this.serverAddress = Address.create(address.getHostAddress(), port);
    this.serverChannel = serverChannel;
    this.serverChannel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
      // close all channels
      inboundChannels.values().forEach(ChannelContext::close);
      inboundChannels.clear();
    });
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
  }
}
