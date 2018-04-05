package io.scalecube.streams;

import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;

import org.slf4j.MDC;
import rx.Observable;

import java.util.function.Consumer;

public final class ClientStreamProcessors {

  private final Config config;

  private final ClientStream clientStream;
  private final ClientStreamProcessorFactory clientStreamProcessorFactory;
  private final StreamMessageDataCodec codec;

  /**
   * Bootstrap constructor.
   *
   * @param config config to set with
   */
  private ClientStreamProcessors(Config config) {
    this.config = config;
    this.codec = config.codec;
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

  public ClientStreamProcessors codec(StreamMessageDataCodec codec) {
    return new ClientStreamProcessors(config.setCodec(codec));
  }

  //// Methods

  public StreamProcessor<StreamMessage, StreamMessage> create(Address address) {
    return create(address, StreamMessage.class);
  }

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
        processor.onNext(codec.encodeData(b));
      }

      @Override
      public Observable<RESP_TYPE> listen() {
        return processor.listen().map(message -> {
          RESP_TYPE data = codec.decodeData(message, respType).data();
          return data;
        }); // StreamMessage -> Pojo
                                                                                              // Payload
      }

      @Override
      public void close() {
        processor.close();
      }
    };
  }

  public <RESP_TYPE> StreamProcessor<StreamMessage, StreamMessage> createRaw(Address address,
      Class<RESP_TYPE> respType) {
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
            .map(message -> {
              if (respType == StreamMessage.class)
                return message;
              else
                return codec.decodeData(message, respType);
            });
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
    private StreamMessageDataCodec codec = new StreamMessageDataCodecImpl();

    private Config() {}

    private Config(Config other, Consumer<Config> modifier) {
      this.bootstrap = other.bootstrap;
      this.codec = other.codec;
      modifier.accept(this);
    }

    private Config setBootstrap(Bootstrap bootstrap) {
      return new Config(this, config -> config.bootstrap = bootstrap);
    }

    public Config setCodec(StreamMessageDataCodec codec) {
      return new Config(this, config -> config.codec = codec);
    }
  }
}
