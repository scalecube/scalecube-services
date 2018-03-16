package io.scalecube.ipc;

import io.scalecube.ipc.netty.NettyServerTransport;
import io.scalecube.transport.Address;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import rx.Observable;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.util.function.Consumer;

public final class ListeningServerStream implements EventStream {

  // listens to bind on server transport
  private final Subject<Address, Address> bindSubject = ReplaySubject.<Address>create(1).toSerialized();
  // listens to server transport channel goes off
  private final Subject<Address, Address> unbindSubject = ReplaySubject.<Address>create(1).toSerialized();

  private final Config config;
  private final ServerStream serverStream = ServerStream.newServerStream();

  private ListeningServerStream(Config config) {
    this.config = config;
  }

  //// Factory

  public static ListeningServerStream newServerStream() {
    return new ListeningServerStream(new Config());
  }

  public ListeningServerStream withListenAddress(String listenAddress) {
    return new ListeningServerStream(config.setListenAddress(listenAddress));
  }

  public ListeningServerStream withListenInterface(String listenInterface) {
    return new ListeningServerStream(config.setListenInterface(listenInterface));
  }

  public ListeningServerStream withPreferIPv6(boolean preferIPv6) {
    return new ListeningServerStream(config.setPreferIPv6(preferIPv6));
  }

  public ListeningServerStream withPort(int port) {
    return new ListeningServerStream(config.setPort(port));
  }

  public ListeningServerStream withPortCount(int portCount) {
    return new ListeningServerStream(config.setPortCount(portCount));
  }

  public ListeningServerStream withPortAutoIncrement(boolean portAutoIncrement) {
    return new ListeningServerStream(config.setPortAutoIncrement(portAutoIncrement));
  }

  public ListeningServerStream withServerBootstrap(ServerBootstrap serverBootstrap) {
    return new ListeningServerStream(config.setServerBootstrap(serverBootstrap));
  }

  @Override
  public void subscribe(ChannelContext channelContext) {
    serverStream.subscribe(channelContext);
  }

  @Override
  public Observable<Event> listen() {
    return serverStream.listen();
  }

  /**
   * Sends a message to client identified by message's senderId, and applying server stream semantic for outbound
   * messages.
   *
   * @param message message to send; must contain valid senderId.
   */
  public void send(ServiceMessage message) {
    serverStream.send(message);
  }

  @Override
  public void close() {
    serverStream.close();
  }

  /**
   * Subscription point for bind operation on internal server transport. NOTE that after calling {@link #close()}
   * subscribing to bind would still result in arriving result.
   */
  public Observable<Address> listenBind() {
    return bindSubject.onBackpressureLatest().asObservable();
  }

  /**
   * Subscription point for server transport goes off and not listening anymore. NOTE that after calling
   * {@link #close()} subscribing to unbind would still result in arriving result.
   */
  public Observable<Address> listenUnbind() {
    return unbindSubject.onBackpressureLatest().asObservable();
  }

  /**
   * Binds internal server transport and start listening on port. Use {@link #listenBind()} to obtain result of this
   * operation. In order to unbind and close server transport call {@link #close()}.
   * 
   * @return new server stream object with with issued bind operation.
   */
  public ListeningServerStream bind() {
    ListeningServerStream serverStream = new ListeningServerStream(config);

    NettyServerTransport serverTransport =
        new NettyServerTransport(config, serverStream::subscribe);

    serverTransport.bind()
        .whenComplete((transport, cause) -> onBind(serverStream, transport, cause));

    return serverStream;
  }

  private void onBind(ListeningServerStream serverStream, NettyServerTransport transport, Throwable cause) {
    if (transport != null) {
      // register cleanup process upfront
      serverStream.listen().subscribe(event -> {
      }, throwable -> unbindTransport(transport), () -> unbindTransport(transport));
      // emit bind success
      transport.getAddress().ifPresent(address -> {
        serverStream.bindSubject.onNext(address);
        serverStream.bindSubject.onCompleted();
      });
    }
    if (cause != null) {
      serverStream.bindSubject.onError(cause);
    }
  }

  private void unbindTransport(NettyServerTransport transport) {
    transport.unbind().whenComplete((transport1, throwable) -> {
      if (transport1 != null) {
        transport1.getAddress().ifPresent(address -> {
          unbindSubject.onNext(address);
          unbindSubject.onCompleted();
        });
      }
      if (throwable != null) {
        unbindSubject.onError(throwable);
      }
    });
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
