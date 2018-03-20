package io.scalecube.streams;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;

import rx.Observable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class StreamProcessors {

  private StreamProcessors() {
    // Do not instantiate
  }

  public static ClientStreamProcessors client() {
    return new ClientStreamProcessors();
  }

  public static ServerStreamProcessors server() {
    return new ServerStreamProcessors();
  }

  public final static class ClientStreamProcessors {

    private final Config config;

    private ClientStream clientStream; // calculated
    private ClientStreamProcessorFactory clientStreamProcessorFactory; // calculated

    private ClientStreamProcessors() {
      this(new Config());
    }

    private ClientStreamProcessors(Config config) {
      this.config = config;
    }

    //// Config

    public ClientStreamProcessors bootstrap(Bootstrap bootstrap) {
      return new ClientStreamProcessors(config.setBootstrap(bootstrap));
    }

    //// Methods

    public ClientStreamProcessors build() {
      // calculating
      clientStream = ClientStream.newClientStream(config.bootstrap);
      clientStreamProcessorFactory = new ClientStreamProcessorFactory(clientStream);
      // return
      return this;
    }

    public StreamProcessor create(Address address) {
      return clientStreamProcessorFactory.newClientStreamProcessor(address);
    }

    public void close() {
      clientStream.close();
      clientStreamProcessorFactory.close();
    }

    //// Config

    private static class Config {

      Bootstrap bootstrap = ClientStream.getDefaultBootstrap();

      private Config() {}

      private Config(Config other, Consumer<Config> modifier) {
        this.bootstrap = other.bootstrap;
        modifier.accept(this);
      }

      private Config setBootstrap(Bootstrap bootstrap) {
        return new Config(this, config -> config.bootstrap = bootstrap);
      }
    }
  }

  public final static class ServerStreamProcessors {

    private final Config config;

    private ServerStreamProcessorFactory serverStreamProcessorFactory; // calculated
    private ListeningServerStream listeningServerStream; // calculated

    private ServerStreamProcessors() {
      this(new Config());
    }

    private ServerStreamProcessors(Config config) {
      this.config = config;
    }

    //// Config

    public ServerStreamProcessors bootstrap(ServerBootstrap bootstrap) {
      return new ServerStreamProcessors(config.setBootstrap(bootstrap));
    }

    public ServerStreamProcessors listenAddress(String listenAddress) {
      return new ServerStreamProcessors(config.setListenAddress(listenAddress));
    }

    public ServerStreamProcessors listenInterface(String listenInterface) {
      return new ServerStreamProcessors(config.setListenInterface(listenInterface));
    }

    public ServerStreamProcessors preferIPv6(boolean preferIPv6) {
      return new ServerStreamProcessors(config.setPreferIPv6(preferIPv6));
    }

    public ServerStreamProcessors port(int port) {
      return new ServerStreamProcessors(config.setPort(port));
    }

    public ServerStreamProcessors portCount(int portCount) {
      return new ServerStreamProcessors(config.setPortCount(portCount));
    }

    public ServerStreamProcessors portAutoIncrement(boolean portAutoIncrement) {
      return new ServerStreamProcessors(config.setPortAutoIncrement(portAutoIncrement));
    }

    //// Methods

    public ServerStreamProcessors build() {
      // calculate
      listeningServerStream = ListeningServerStream.newListeningServerStream(config.embeddedConfig);
      serverStreamProcessorFactory = new ServerStreamProcessorFactory(listeningServerStream);
      // return
      return this;
    }

    public Address bindAwait() {
      return listeningServerStream.bindAwait();
    }

    public CompletableFuture<Address> bind() {
      return listeningServerStream.bind();
    }

    public void unbind() {
      listeningServerStream.close();
    }

    public Observable<StreamProcessor> listen() {
      return serverStreamProcessorFactory.listenServerStreamProcessor();
    }

    public void close() {
      listeningServerStream.close();
      serverStreamProcessorFactory.close();
    }

    //// Config

    private static class Config {

      private ListeningServerStream.Config embeddedConfig = ListeningServerStream.Config.newConfig();

      private Config() {}

      private Config(Config other, Consumer<Config> modifier) {
        this.embeddedConfig = other.embeddedConfig;
        modifier.accept(this);
      }

      private Config setBootstrap(ServerBootstrap bootstrap) {
        return new Config(this, config -> config.embeddedConfig.setServerBootstrap(bootstrap));
      }

      public Config setListenAddress(String listenAddress) {
        return new Config(this, config -> config.embeddedConfig.setListenAddress(listenAddress));
      }

      public Config setListenInterface(String listenInterface) {
        return new Config(this, config -> config.embeddedConfig.setListenInterface(listenInterface));
      }

      public Config setPreferIPv6(boolean preferIPv6) {
        return new Config(this, config -> config.embeddedConfig.setPreferIPv6(preferIPv6));
      }

      public Config setPort(int port) {
        return new Config(this, config -> config.embeddedConfig.setPort(port));
      }

      public Config setPortCount(int portCount) {
        return new Config(this, config -> config.embeddedConfig.setPortCount(portCount));
      }

      public Config setPortAutoIncrement(boolean portAutoIncrement) {
        return new Config(this, config -> config.embeddedConfig.setPortAutoIncrement(portAutoIncrement));
      }
    }
  }
}
