package io.scalecube.streams;

import io.scalecube.transport.Address;

import io.netty.bootstrap.ServerBootstrap;

import rx.Observable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class ServerStreamProcessors {

  private final Config config;

  private final ListeningServerStream listeningServerStream;
  private final ServerStreamProcessorFactory serverStreamProcessorFactory;

  /**
   * Bootstrap constructor.
   *
   * @param config config to set with
   */
  private ServerStreamProcessors(Config config) {
    this.config = config;
    this.listeningServerStream = ListeningServerStream.newListeningServerStream(config.innerConfig);
    this.serverStreamProcessorFactory = new ServerStreamProcessorFactory(listeningServerStream);
  }

  /**
   * Prototype constructor.
   *
   * @param other instance to copy property references from
   * @param config config to set with
   */
  private ServerStreamProcessors(ServerStreamProcessors other, Config config) {
    this.config = config;
    this.listeningServerStream = other.listeningServerStream;
    this.serverStreamProcessorFactory = other.serverStreamProcessorFactory;
  }

  //// Factory and config

  public static ServerStreamProcessors newServerStreamProcessors() {
    return new ServerStreamProcessors(new Config());
  }

  public ServerStreamProcessors bootstrap(ServerBootstrap bootstrap) {
    return new ServerStreamProcessors(this, config.setBootstrap(bootstrap));
  }

  public ServerStreamProcessors listenAddress(String listenAddress) {
    return new ServerStreamProcessors(this, config.setListenAddress(listenAddress));
  }

  public ServerStreamProcessors listenInterface(String listenInterface) {
    return new ServerStreamProcessors(this, config.setListenInterface(listenInterface));
  }

  public ServerStreamProcessors preferIPv6(boolean preferIPv6) {
    return new ServerStreamProcessors(this, config.setPreferIPv6(preferIPv6));
  }

  public ServerStreamProcessors port(int port) {
    return new ServerStreamProcessors(this, config.setPort(port));
  }

  public ServerStreamProcessors portCount(int portCount) {
    return new ServerStreamProcessors(this, config.setPortCount(portCount));
  }

  public ServerStreamProcessors portAutoIncrement(boolean portAutoIncrement) {
    return new ServerStreamProcessors(this, config.setPortAutoIncrement(portAutoIncrement));
  }

  //// Methods

  public Address bindAwait() {
    return prepareBind().bindAwait();
  }

  public CompletableFuture<Address> bind() {
    return prepareBind().bind();
  }

  private ListeningServerStream prepareBind() {
    return listeningServerStream
        .withPort(config.innerConfig.getPort())
        .withPortAutoIncrement(config.innerConfig.isPortAutoIncrement())
        .withPortCount(config.innerConfig.getPortCount())
        .withListenAddress(config.innerConfig.getListenAddress())
        .withListenInterface(config.innerConfig.getListenInterface())
        .withPreferIPv6(config.innerConfig.isPreferIPv6())
        .withServerBootstrap(config.innerConfig.getServerBootstrap());
  }

  public void unbind() {
    listeningServerStream.close();
  }

  public Observable<StreamProcessor> listen() {
    return serverStreamProcessorFactory.listen();
  }

  public void close() {
    listeningServerStream.close();
    serverStreamProcessorFactory.close();
  }

  //// Config

  private static class Config {

    private ListeningServerStream.Config innerConfig = ListeningServerStream.Config.newConfig();

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.innerConfig = other.innerConfig;
      modifier.accept(this);
    }

    private Config setBootstrap(ServerBootstrap bootstrap) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setServerBootstrap(bootstrap));
    }

    private Config setListenAddress(String listenAddress) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setListenAddress(listenAddress));
    }

    private Config setListenInterface(String listenInterface) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setListenInterface(listenInterface));
    }

    private Config setPreferIPv6(boolean preferIPv6) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setPreferIPv6(preferIPv6));
    }

    private Config setPort(int port) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setPort(port));
    }

    private Config setPortCount(int portCount) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setPortCount(portCount));
    }

    private Config setPortAutoIncrement(boolean portAutoIncrement) {
      return new Config(this, //
          config -> config.innerConfig = config.innerConfig.setPortAutoIncrement(portAutoIncrement));
    }
  }
}
