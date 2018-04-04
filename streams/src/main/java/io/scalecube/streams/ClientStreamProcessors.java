package io.scalecube.streams;

import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;

import rx.Observable;

import java.util.function.Consumer;

public final class ClientStreamProcessors {

  private final Config config;

  private final ClientStream clientStream;
  private final ClientStreamProcessorFactory clientStreamProcessorFactory;
  private StreamMessageDataCodec codec;

  /**
   * Bootstrap constructor.
   *
   * @param config config to set with
   */
  private ClientStreamProcessors(Config config) {
    this.config = config;
    this.clientStream = ClientStream.newClientStream(config.bootstrap);
    this.clientStreamProcessorFactory = new ClientStreamProcessorFactory(clientStream);
  }

  //// Factory and config

  public static ClientStreamProcessors newClientStreamProcessors() {
    return new ClientStreamProcessors(new Config());
  }

  public ClientStreamProcessors bootstrap(Bootstrap bootstrap) {
    return new ClientStreamProcessors(config.setBootstrap(bootstrap));
  }

  //// Methods

  public <RESP_TYPE> StreamProcessor<StreamMessage, RESP_TYPE> create(Address address, Class<RESP_TYPE> respType) {

    // noinspection unchecked
    StreamProcessor<StreamMessage, StreamMessage> processor =
        clientStreamProcessorFactory.newClientStreamProcessor(address);

    return new StreamProcessor<StreamMessage, RESP_TYPE>() {
      @Override
      public void onCompleted() {
        processor.onCompleted();
      }

      @Override
      public void onError(Throwable e) {
        processor.onError(e);
      }

      @Override
      public void onNext(StreamMessage b) {
        processor.onNext(StreamMessage.from(b).data(codec.encodeData(b)).build());
      }

      @Override
      public Observable<RESP_TYPE> listen() {
        return processor.listen().map(message -> codec.decodeData(message, respType).data()); // StreamMessage -> Pojo
                                                                                              // Payload
      }

      @Override
      public void close() {
        processor.close();
      }
    };
  }

  public <B> StreamProcessor<StreamMessage, StreamMessage> createRaw(Address address, Class<B> respType) {

    // noinspection unchecked
    StreamProcessor<StreamMessage, StreamMessage> processor =
        clientStreamProcessorFactory.newClientStreamProcessor(address);

    return new StreamProcessor<StreamMessage, StreamMessage>() {
      @Override
      public void onCompleted() {
        processor.onCompleted();
      }

      @Override
      public void onError(Throwable e) {
        processor.onError(e);
      }

      @Override
      public void onNext(StreamMessage msg) {// Payload encoded
        processor.onNext(codec.encodeData(msg));
      }

      @Override
      public Observable<StreamMessage> listen() { // Payload decoded
        return processor.listen()
            .map(message -> StreamMessage.from(message).data(codec.decodeData(message, respType)).build());
      }

      @Override
      public void close() {
        processor.close();
      }
    };
  }

  public void close() {
    clientStream.close();
    clientStreamProcessorFactory.close();
  }

  //// Config

  private static class Config {

    private Bootstrap bootstrap = ClientStream.getDefaultBootstrap();

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
