package io.scalecube.gateway.socketio;

import io.scalecube.ipc.ServerStream;
import io.scalecube.socketio.ServerConfiguration;
import io.scalecube.socketio.SocketIOServer;

import io.netty.bootstrap.ServerBootstrap;

import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

/**
 * Gateway server on socketio.
 */
public final class GatewaySocketIoServer {

  private final Config config;

  private SocketIOServer socketioServer; // calculated

  private GatewaySocketIoServer(Config config) {
    this.config = config;
  }

  //// Factory

  public static GatewaySocketIoServer onPort(int port, ServerStream serverStream) {
    Config config = new Config();
    config.port = port;
    config.serverStream = serverStream;
    return new GatewaySocketIoServer(config);
  }

  public GatewaySocketIoServer withServerBootstrap(ServerBootstrap serverBootstrap) {
    return new GatewaySocketIoServer(config.setServerBootstrap(serverBootstrap));
  }

  public GatewaySocketIoServer withSsl(SSLContext sslContext) {
    return new GatewaySocketIoServer(config.setSslContext(sslContext));
  }

  public GatewaySocketIoServer withCloseTimeout(int closeTimeout) {
    return new GatewaySocketIoServer(config.setCloseTimeout(closeTimeout));
  }

  public GatewaySocketIoServer withHeartbeatInterval(int heartbeatInterval) {
    return new GatewaySocketIoServer(config.setHeartbeatInterval(heartbeatInterval));
  }

  public GatewaySocketIoServer withHeartbeatTimeout(int heartbeatTimeout) {
    return new GatewaySocketIoServer(config.setHeartbeatTimeout(heartbeatTimeout));
  }

  public GatewaySocketIoServer withMaxWebSocketFrameSize(int maxWebSocketFrameSize) {
    return new GatewaySocketIoServer(config.setMaxWebSocketFrameSize(maxWebSocketFrameSize));
  }

  public GatewaySocketIoServer withAlwaysSecureWebSocketLocation(boolean alwaysSecureWebSocketLocation) {
    return new GatewaySocketIoServer(config.setAlwaysSecureWebSocketLocation(alwaysSecureWebSocketLocation));
  }

  public GatewaySocketIoServer withRemoteAddressHeader(String remoteAddressHeader) {
    return new GatewaySocketIoServer(config.setRemoteAddressHeader(remoteAddressHeader));
  }

  public GatewaySocketIoServer withTransports(String transports) {
    return new GatewaySocketIoServer(config.setTransports(transports));
  }

  //// Bootstrap

  /**
   * Starts socketio server.
   */
  public synchronized void start() {
    if (socketioServer != null && socketioServer.isStarted()) {
      throw new IllegalStateException("Failed to start server: already started");
    }

    ServerConfiguration configuration = ServerConfiguration.builder()
        .port(config.port)
        .sslContext(config.sslContext)
        .closeTimeout(config.closeTimeout)
        .alwaysSecureWebSocketLocation(config.alwaysSecureWebSocketLocation)
        .heartbeatInterval(config.heartbeatInterval)
        .heartbeatTimeout(config.heartbeatTimeout)
        .maxWebSocketFrameSize(config.maxWebSocketFrameSize)
        .remoteAddressHeader(config.remoteAddressHeader)
        .transports(config.transports)
        .eventExecutorEnabled(Config.EVENT_EXECUTOR_ENABLED)
        .build();

    SocketIOServer server = SocketIOServer.newInstance(configuration);
    server.setListener(new GatewaySocketIoListener(config.serverStream));
    if (config.serverBootstrap != null) {
      server.setServerBootstrapFactory(() -> config.serverBootstrap);
    }
    server.start();

    socketioServer = server;
  }

  /**
   * Stops socketio server.
   */
  public synchronized void stop() {
    if (socketioServer != null && socketioServer.isStopped()) {
      throw new IllegalStateException("Failed to stop server: already stopped");
    }
    if (socketioServer != null) {
      socketioServer.stop();
    }
  }

  //// Config

  public static class Config {

    private static final int DEFAULT_MAX_FRAME_LENGTH = 65536;
    private static final int SOCKETIO_DEFAULT_HEARTBEAT_TIMEOUT = 60;
    private static final int SOCKETIO_DEFAULT_HEARTBEAT_INTERVAL = 25;
    private static final int SOCKETIO_DEFAULT_CLOSE_TIMEOUT = 60;
    private static final String SOCKETIO_DEFAULT_TRANSPORTS = "websocket,flashsocket,xhr-polling,jsonp-polling";
    private static final boolean SOCKETIO_DEFAULT_ALWAYS_SECURE_WEB_SOCKET_LOCATION = false;
    private static final String SOCKETIO_DEFAULT_REMOTE_ADDRESS_HEADER = "X-Forwarded-For";
    private static final boolean EVENT_EXECUTOR_ENABLED = false;

    private SSLContext sslContext;
    private int port;
    private String transports = SOCKETIO_DEFAULT_TRANSPORTS;
    private int heartbeatTimeout = SOCKETIO_DEFAULT_HEARTBEAT_TIMEOUT;
    private int heartbeatInterval = SOCKETIO_DEFAULT_HEARTBEAT_INTERVAL;
    private int closeTimeout = SOCKETIO_DEFAULT_CLOSE_TIMEOUT;
    private int maxWebSocketFrameSize = DEFAULT_MAX_FRAME_LENGTH;
    private boolean alwaysSecureWebSocketLocation = SOCKETIO_DEFAULT_ALWAYS_SECURE_WEB_SOCKET_LOCATION;
    private String remoteAddressHeader = SOCKETIO_DEFAULT_REMOTE_ADDRESS_HEADER;
    private ServerStream serverStream;
    private ServerBootstrap serverBootstrap = null;

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.sslContext = other.sslContext;
      this.port = other.port;
      this.transports = other.transports;
      this.heartbeatTimeout = other.heartbeatTimeout;
      this.heartbeatInterval = other.heartbeatInterval;
      this.closeTimeout = other.closeTimeout;
      this.maxWebSocketFrameSize = other.maxWebSocketFrameSize;
      this.alwaysSecureWebSocketLocation = other.alwaysSecureWebSocketLocation;
      this.remoteAddressHeader = other.remoteAddressHeader;
      this.serverStream = other.serverStream;
      this.serverBootstrap = other.serverBootstrap;
      modifier.accept(this);
    }

    public SSLContext getSslContext() {
      return sslContext;
    }

    public Config setSslContext(SSLContext sslContext) {
      return new Config(this, config -> config.sslContext = sslContext);
    }

    public int getPort() {
      return port;
    }

    public Config setPort(int port) {
      return new Config(this, config -> config.port = port);
    }

    public String getTransports() {
      return transports;
    }

    public Config setTransports(String transports) {
      return new Config(this, config -> config.transports = transports);
    }

    public int getHeartbeatTimeout() {
      return heartbeatTimeout;
    }

    public Config setHeartbeatTimeout(int heartbeatTimeout) {
      return new Config(this, config -> config.heartbeatTimeout = heartbeatTimeout);
    }

    public int getHeartbeatInterval() {
      return heartbeatInterval;
    }

    public Config setHeartbeatInterval(int heartbeatInterval) {
      return new Config(this, config -> config.heartbeatInterval = heartbeatInterval);
    }

    public int getCloseTimeout() {
      return closeTimeout;
    }

    public Config setCloseTimeout(int closeTimeout) {
      return new Config(this, config -> config.closeTimeout = closeTimeout);
    }

    public int getMaxWebSocketFrameSize() {
      return maxWebSocketFrameSize;
    }

    public Config setMaxWebSocketFrameSize(int maxWebSocketFrameSize) {
      return new Config(this, config -> config.maxWebSocketFrameSize = maxWebSocketFrameSize);
    }

    public boolean isAlwaysSecureWebSocketLocation() {
      return alwaysSecureWebSocketLocation;
    }

    public Config setAlwaysSecureWebSocketLocation(boolean alwaysSecureWebSocketLocation) {
      return new Config(this, config -> config.alwaysSecureWebSocketLocation = alwaysSecureWebSocketLocation);
    }

    public String getRemoteAddressHeader() {
      return remoteAddressHeader;
    }

    public Config setRemoteAddressHeader(String remoteAddressHeader) {
      return new Config(this, config -> config.remoteAddressHeader = remoteAddressHeader);
    }

    public ServerStream getServerStream() {
      return serverStream;
    }

    public Config setServerStream(ServerStream serverStream) {
      return new Config(this, config -> config.serverStream = serverStream);
    }

    public ServerBootstrap getServerBootstrap() {
      return serverBootstrap;
    }

    public Config setServerBootstrap(ServerBootstrap serverBootstrap) {
      return new Config(this, config -> config.serverBootstrap = serverBootstrap);
    }
  }
}
