package io.scalecube.streams;

import io.scalecube.streams.exceptions.DefaultStreamExceptionMapper;
import io.scalecube.streams.exceptions.StreamExceptionMapper;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;

import java.util.function.Consumer;

public final class ClientStreamProcessors {

  private final Config config;

  private final ClientStream clientStream;
  private final ClientStreamProcessorFactory clientStreamProcessorFactory;

  /**
   * Bootstrap constructor.
   *
   * @param config config to set with
   */
  private ClientStreamProcessors(Config config) {
    this.config = config;
    this.clientStream = ClientStream.newClientStream(config.bootstrap);
    this.clientStreamProcessorFactory = new ClientStreamProcessorFactory(clientStream, config.exceptionMapper);
  }

  //// Factory and config

  public static ClientStreamProcessors newClientStreamProcessors() {
    return new ClientStreamProcessors(new Config());
  }

  public ClientStreamProcessors bootstrap(Bootstrap bootstrap) {
    return new ClientStreamProcessors(config.setBootstrap(bootstrap));
  }

  public ClientStreamProcessors exceptionMapper(StreamExceptionMapper exceptionMapper) {
    return new ClientStreamProcessors(config.setExceptionMapper(exceptionMapper));
  }

  //// Methods

  public StreamProcessor create(Address address) {
    return clientStreamProcessorFactory.newClientStreamProcessor(address);
  }

  public void close() {
    clientStream.close();
    clientStreamProcessorFactory.close();
  }

  //// Config

  private static class Config {

    private Bootstrap bootstrap = ClientStream.getDefaultBootstrap();
    private StreamExceptionMapper exceptionMapper = new DefaultStreamExceptionMapper();

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.bootstrap = other.bootstrap;
      modifier.accept(this);
    }

    private Config setBootstrap(Bootstrap bootstrap) {
      return new Config(this, config -> config.bootstrap = bootstrap);
    }

    private Config setExceptionMapper(StreamExceptionMapper exceptionMapper) {
      return new Config(this, config -> config.exceptionMapper = exceptionMapper);
    }
  }
}
