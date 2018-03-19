package io.scalecube.streams;

import io.scalecube.transport.Address;

import rx.Subscriber;
import rx.Subscription;
import rx.observers.Subscribers;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class StreamProcessors {

  private final Config config;
  // client side
  private final ClientStream clientStream;
  private final ClientStreamProcessorFactory clientStreamProcessorFactory;
  // server side
  private final ListeningServerStream listeningServerStream;
  private final ServerStreamProcessorFactory serverStreamProcessorFactory;

  /**
   * Bootstrap constructor.
   * 
   * @param config config to set with
   */
  private StreamProcessors(Config config) {
    this.config = config;

    this.clientStream = ClientStream.newClientStream();
    this.clientStreamProcessorFactory =
        ClientStreamProcessorFactory.newClientStreamProcessorFactory(clientStream);

    this.listeningServerStream = ListeningServerStream.newListeningServerStream();
    this.serverStreamProcessorFactory =
        ServerStreamProcessorFactory.newServerStreamProcessorFactory(listeningServerStream);
  }

  /**
   * Prototype constructor.
   *
   * @param other instance to copy property references from
   * @param config config to set with
   */
  private StreamProcessors(StreamProcessors other, Config config) {
    this.config = config;
    this.clientStream = other.clientStream;
    this.clientStreamProcessorFactory = other.clientStreamProcessorFactory;
    this.listeningServerStream = other.listeningServerStream;
    this.serverStreamProcessorFactory = other.serverStreamProcessorFactory;
  }

  //// Factory and config

  public static StreamProcessors newStreamProcessors() {
    return new StreamProcessors(new Config());
  }

  public StreamProcessors withListenAddress(String listenAddress) {
    return new StreamProcessors(this, config.setListenAddress(listenAddress));
  }

  public StreamProcessors withListenInterface(String listenInterface) {
    return new StreamProcessors(this, config.setListenInterface(listenInterface));
  }

  public StreamProcessors withPort(int port) {
    return new StreamProcessors(this, config.setPort(port));
  }

  //// Methods

  public StreamProcessor client(Address address) {
    return clientStreamProcessorFactory.newClientStreamProcessor(address);
  }

  public Subscription server(Consumer<StreamProcessor> consumer) {
    Subscriber<StreamProcessor> subscriber = Subscribers.create(consumer::accept);
    return serverStreamProcessorFactory.listenServerStreamProcessor().subscribe(subscriber);
  }

  public CompletableFuture<Address> bind() {
    return listeningServerStream
        .withListenAddress(config.getListenAddress())
        .withListenInterface(config.getListenInterface())
        .withPort(config.getPort())
        .bind();
  }

  public void close() {
    clientStream.close();
    clientStreamProcessorFactory.close();
    listeningServerStream.close();
    serverStreamProcessorFactory.close();
  }

  //// Config

  public static class Config {

    private static final String DEFAULT_LISTEN_ADDRESS = null;
    private static final String DEFAULT_LISTEN_INTERFACE = null; // Default listen settings fallback to getLocalHost
    private static final int DEFAULT_PORT = 5801;

    private String listenAddress = DEFAULT_LISTEN_ADDRESS;
    private String listenInterface = DEFAULT_LISTEN_INTERFACE;
    private int port = DEFAULT_PORT;

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.listenAddress = other.listenAddress;
      this.listenInterface = other.listenInterface;
      this.port = other.port;
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

    public int getPort() {
      return port;
    }

    public Config setPort(int port) {
      return new Config(this, config -> config.port = port);
    }
  }
}
