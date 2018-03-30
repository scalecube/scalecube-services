package io.scalecube.streams;

import io.scalecube.streams.netty.NettyServerTransport;
import io.scalecube.transport.Address;

import com.google.common.base.Throwables;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import rx.Observable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class ListeningServerStream implements EventStream {

  private final Config config;
  private final ServerStream serverStream;

  /**
   * Bootstrap constructor.
   * 
   * @param config config to set with
   */
  private ListeningServerStream(Config config) {
    this.config = config;
    this.serverStream = ServerStream.newServerStream();
  }

  /**
   * Prototype constructor.
   * 
   * @param other instance to copy property references from
   * @param config config to set with
   */
  private ListeningServerStream(ListeningServerStream other, Config config) {
    this.config = config;
    this.serverStream = other.serverStream;
  }

  //// Factory and config

  public static ListeningServerStream newListeningServerStream() {
    return newListeningServerStream(new Config());
  }

  public static ListeningServerStream newListeningServerStream(Config config) {
    return new ListeningServerStream(config);
  }

  public ListeningServerStream withListenAddress(String listenAddress) {
    return new ListeningServerStream(this, config.setListenAddress(listenAddress));
  }

  public ListeningServerStream withListenInterface(String listenInterface) {
    return new ListeningServerStream(this, config.setListenInterface(listenInterface));
  }

  public ListeningServerStream withPreferIPv6(boolean preferIPv6) {
    return new ListeningServerStream(this, config.setPreferIPv6(preferIPv6));
  }

  public ListeningServerStream withPort(int port) {
    return new ListeningServerStream(this, config.setPort(port));
  }

  public ListeningServerStream withPortCount(int portCount) {
    return new ListeningServerStream(this, config.setPortCount(portCount));
  }

  public ListeningServerStream withPortAutoIncrement(boolean portAutoIncrement) {
    return new ListeningServerStream(this, config.setPortAutoIncrement(portAutoIncrement));
  }

  public ListeningServerStream withServerBootstrap(ServerBootstrap serverBootstrap) {
    return new ListeningServerStream(this, config.setServerBootstrap(serverBootstrap));
  }

  //// Methods

  @Override
  public void subscribe(ChannelContext channelContext) {
    serverStream.subscribe(channelContext);
  }

  @Override
  public Observable<Event> listen() {
    return serverStream.listen();
  }

  @Override
  public void onNext(Event event) {
    serverStream.onNext(event);
  }

  @Override
  public void onNext(Address address, Event event) {
    serverStream.onNext(address, event);
  }

  /**
   * Sends a message to client identified by message's subject, and applying server stream semantic for outbound
   * messages.
   *
   * @param message message to send; must contain valid subject.
   */
  public void send(StreamMessage message) {
    serverStream.send(message);
  }

  @Override
  public void close() {
    serverStream.close();
  }

  @Override
  public void listenClose(Consumer<Void> onClose) {
    serverStream.listenClose(onClose);
  }

  /**
   * Binds asynchronously according to config object that is present at the moment of bind operation.
   * 
   * @return listening server address
   */
  public CompletableFuture<Address> bind() {
    CompletableFuture<Address> promise = new CompletableFuture<>();
    NettyServerTransport serverTransport = new NettyServerTransport(config, serverStream::subscribe);
    serverTransport.bind().whenComplete((serverTransport1, throwable) -> {
      if (serverTransport1 != null) {
        // register cleanup process upfront
        serverStream.listenClose(aVoid -> serverTransport1.unbind());
        // complete promise
        serverTransport1.getServerAddress().ifPresent(promise::complete);
      }
      if (throwable != null) {
        promise.completeExceptionally(throwable);
      }
    });
    return promise;
  }

  /**
   * Binds synchronously.
   * 
   * @return bound address
   */
  public Address bindAwait() {
    try {
      return bind().get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  //// Config

  public static class Config {

    private static final String DEFAULT_LISTEN_ADDRESS = null;
    private static final String DEFAULT_LISTEN_INTERFACE = null; // Default listen settings fallback to getLocalHost
    private static final boolean DEFAULT_PREFER_IP6 = false;
    private static final int DEFAULT_PORT = 5801;
    private static final int DEFAULT_PORT_COUNT = 100;
    private static final boolean DEFAULT_PORT_AUTO_INCREMENT = true;

    private static final ServerBootstrap DEFAULT_SERVER_BOOTSTRAP;
    // Pre-configure default server bootstrap
    static {
      DEFAULT_SERVER_BOOTSTRAP = new ServerBootstrap()
          .group(new NioEventLoopGroup(1), new NioEventLoopGroup(0))
          .channel(NioServerSocketChannel.class)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .childOption(ChannelOption.SO_REUSEADDR, true);
    }

    private String listenAddress = DEFAULT_LISTEN_ADDRESS;
    private String listenInterface = DEFAULT_LISTEN_INTERFACE;
    private boolean preferIPv6 = DEFAULT_PREFER_IP6;
    private int port = DEFAULT_PORT;
    private int portCount = DEFAULT_PORT_COUNT;
    private boolean portAutoIncrement = DEFAULT_PORT_AUTO_INCREMENT;
    private ServerBootstrap serverBootstrap = DEFAULT_SERVER_BOOTSTRAP;

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.listenAddress = other.listenAddress;
      this.listenInterface = other.listenInterface;
      this.preferIPv6 = other.preferIPv6;
      this.port = other.port;
      this.portCount = other.portCount;
      this.portAutoIncrement = other.portAutoIncrement;
      this.serverBootstrap = other.serverBootstrap;
      modifier.accept(this);
    }

    public static Config newConfig() {
      return new Config();
    }

    public String getListenAddress() {
      return listenAddress;
    }

    public Config setListenAddress(String listenAddress) {
      return new Config(this, config -> config.listenAddress = listenAddress);
    }

    public String getListenInterface() {
      return listenInterface;
    }

    public Config setListenInterface(String listenInterface) {
      return new Config(this, config -> config.listenInterface = listenInterface);
    }

    public boolean isPreferIPv6() {
      return preferIPv6;
    }

    public Config setPreferIPv6(boolean preferIPv6) {
      return new Config(this, config -> config.preferIPv6 = preferIPv6);
    }

    public int getPort() {
      return port;
    }

    public Config setPort(int port) {
      return new Config(this, config -> config.port = port);
    }

    public int getPortCount() {
      return portCount;
    }

    public Config setPortCount(int portCount) {
      return new Config(this, config -> config.portCount = portCount);
    }

    public boolean isPortAutoIncrement() {
      return portAutoIncrement;
    }

    public Config setPortAutoIncrement(boolean portAutoIncrement) {
      return new Config(this, config -> config.portAutoIncrement = portAutoIncrement);
    }

    public ServerBootstrap getServerBootstrap() {
      return serverBootstrap;
    }

    public Config setServerBootstrap(ServerBootstrap serverBootstrap) {
      return new Config(this, config -> config.serverBootstrap = serverBootstrap);
    }
  }
}
