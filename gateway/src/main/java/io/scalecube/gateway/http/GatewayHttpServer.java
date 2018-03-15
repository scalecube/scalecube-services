package io.scalecube.gateway.http;

import io.scalecube.ipc.ServerStream;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

/**
 * Gateway server on http.
 */
public final class GatewayHttpServer {

  private final Config config;

  private Channel serverChannel; // calculated

  private boolean started = false;

  private GatewayHttpServer(Config config) {
    this.config = config;
  }

  //// Factory

  public static GatewayHttpServer onPort(int port, ServerStream serverStream) {
    Config config = new Config();
    config.port = port;
    config.serverStream = serverStream;
    return new GatewayHttpServer(config);
  }

  public GatewayHttpServer withServerBootstrap(ServerBootstrap serverBootstrap) {
    return new GatewayHttpServer(config.setServerBootstrap(serverBootstrap));
  }

  public GatewayHttpServer withSsl(SSLContext sslContext) {
    return new GatewayHttpServer(config.setSslContext(sslContext));
  }

  public GatewayHttpServer withMaxFrameLength(int maxFrameLength) {
    return new GatewayHttpServer(config.setMaxFrameLength(maxFrameLength));
  }

  public GatewayHttpServer withCorsEnabled(boolean corsEnabled) {
    return new GatewayHttpServer(config.setCorsEnabled(corsEnabled));
  }

  public GatewayHttpServer withAccessControlAllowOrigin(String accessControlAllowOrigin) {
    return new GatewayHttpServer(config.setAccessControlAllowOrigin(accessControlAllowOrigin));
  }

  public GatewayHttpServer withAccessControlAllowMethods(String accessControlAllowMethods) {
    return new GatewayHttpServer(config.setAccessControlAllowMethods(accessControlAllowMethods));
  }

  public GatewayHttpServer withAccessControlMaxAge(int accessControlMaxAge) {
    return new GatewayHttpServer(config.setAccessControlMaxAge(accessControlMaxAge));
  }

  //// Bootstrap

  /**
   * Starts http server.
   */
  public synchronized void start() {
    if (started) {
      throw new IllegalStateException("Failed to start server: already started");
    }

    ServerBootstrap serverBootstrap = config.serverBootstrap;

    serverChannel = serverBootstrap
        .childHandler(new GatewayHttpChannelInitializer(config))
        .bind(new InetSocketAddress(config.port))
        .syncUninterruptibly()
        .channel();

    started = true;
  }

  /**
   * Stops http server.
   */
  public synchronized void stop() {
    if (!started) {
      throw new IllegalStateException("Failed to stop server: already stopped");
    }
    serverChannel.close().syncUninterruptibly();
    started = false;
  }

  //// Config

  public static class Config implements Cloneable {

    private static final int DEFAULT_MAX_FRAME_LENGTH = 2048000;
    private static final boolean DEFAULT_CORS_ENABLED = false;
    private static final String DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN = "*";
    private static final String DEFAULT_ACCESS_CONTROL_ALLOW_METHODS = "GET, POST, OPTIONS";
    private static final int DEFAULT_ACCESS_CONTROL_MAX_AGE = 86400;

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

    private SSLContext sslContext;
    private int port;
    private int maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;
    private boolean corsEnabled = DEFAULT_CORS_ENABLED;
    private String accessControlAllowOrigin = DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN;
    private String accessControlAllowMethods = DEFAULT_ACCESS_CONTROL_ALLOW_METHODS;
    private int accessControlMaxAge = DEFAULT_ACCESS_CONTROL_MAX_AGE;
    private ServerStream serverStream;
    private ServerBootstrap serverBootstrap = DEFAULT_SERVER_BOOTSTRAP;

    private Config() {}

    @Override
    protected Config clone() {
      Config clone = new Config();
      clone.sslContext = sslContext;
      clone.port = port;
      clone.maxFrameLength = maxFrameLength;
      clone.corsEnabled = corsEnabled;
      clone.accessControlAllowOrigin = accessControlAllowOrigin;
      clone.accessControlAllowMethods = accessControlAllowMethods;
      clone.accessControlMaxAge = accessControlMaxAge;
      clone.serverStream = serverStream;
      clone.serverBootstrap = serverBootstrap;
      return clone;
    }

    private Config copyAndSet(Consumer<Config> consumer) {
      Config clone;
      consumer.accept(clone = this.clone());
      return clone;
    }

    public SSLContext getSslContext() {
      return sslContext;
    }

    public Config setSslContext(SSLContext sslContext) {
      return copyAndSet(config1 -> config1.sslContext = sslContext);
    }

    public int getPort() {
      return port;
    }

    public Config setPort(int port) {
      return copyAndSet(config1 -> config1.port = port);
    }

    public int getMaxFrameLength() {
      return maxFrameLength;
    }

    public Config setMaxFrameLength(int maxFrameLength) {
      return copyAndSet(config1 -> config1.maxFrameLength = maxFrameLength);
    }

    public boolean isCorsEnabled() {
      return corsEnabled;
    }

    public Config setCorsEnabled(boolean corsEnabled) {
      return copyAndSet(config1 -> config1.corsEnabled = corsEnabled);
    }

    public String getAccessControlAllowOrigin() {
      return accessControlAllowOrigin;
    }

    public Config setAccessControlAllowOrigin(String accessControlAllowOrigin) {
      return copyAndSet(config1 -> config1.accessControlAllowOrigin = accessControlAllowOrigin);
    }

    public String getAccessControlAllowMethods() {
      return accessControlAllowMethods;
    }

    public Config setAccessControlAllowMethods(String accessControlAllowMethods) {
      return copyAndSet(config1 -> config1.accessControlAllowMethods = accessControlAllowMethods);
    }

    public int getAccessControlMaxAge() {
      return accessControlMaxAge;
    }

    public Config setAccessControlMaxAge(int accessControlMaxAge) {
      return copyAndSet(config1 -> config1.accessControlMaxAge = accessControlMaxAge);
    }

    public ServerStream getServerStream() {
      return serverStream;
    }

    public Config setServerStream(ServerStream serverStream) {
      return copyAndSet(config1 -> config1.serverStream = serverStream);
    }

    public ServerBootstrap getServerBootstrap() {
      return serverBootstrap;
    }

    public Config setServerBootstrap(ServerBootstrap serverBootstrap) {
      return copyAndSet(config1 -> config1.serverBootstrap = serverBootstrap);
    }
  }
}
