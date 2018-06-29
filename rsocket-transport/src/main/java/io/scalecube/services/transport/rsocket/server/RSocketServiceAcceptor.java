package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.HeadAndTail;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodRegistry;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final ServiceMessageCodec messageCodec;
  private final ServiceMethodRegistry methodRegistry;

  public RSocketServiceAcceptor(ServiceMessageCodec codec, ServiceMethodRegistry methodRegistry) {
    this.messageCodec = codec;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket socket) {
    LOGGER.info("Accepted rSocket: {}, connectionSetup: {}", socket, setup);

    return Mono.just(new AbstractRSocket() {
      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        return Mono.just(payload)
            .map(this::toMessage)
            .doOnNext(this::checkMethodInvokerExist)
            .flatMap(message -> methodRegistry.getInvoker(message.qualifier())
                .invokeOne(message, ServiceMessageCodec::decodeData))
            .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)))
            .map(this::toPayload);
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        return Flux.just(payload)
            .map(this::toMessage)
            .doOnNext(this::checkMethodInvokerExist)
            .flatMap(message -> methodRegistry.getInvoker(message.qualifier())
                .invokeMany(message, ServiceMessageCodec::decodeData))
            .onErrorResume(t -> Flux.just(ExceptionProcessor.toMessage(t)))
            .map(this::toPayload);
      }

      @Override
      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(HeadAndTail.createFrom(Flux.from(payloads).map(this::toMessage)))
            .flatMap(pair -> {
              ServiceMessage message = pair.head();
              checkMethodInvokerExist(message);
              Flux<ServiceMessage> messages = Flux.from(pair.tail()).startWith(message);
              return methodRegistry.getInvoker(message.qualifier())
                  .invokeBidirectional(messages, ServiceMessageCodec::decodeData);
            })
            .onErrorResume(t -> Flux.just(ExceptionProcessor.toMessage(t)))
            .map(this::toPayload);
      }

      private Payload toPayload(ServiceMessage response) {
        return messageCodec.encodeAndTransform(response, ByteBufPayload::create);
      }

      private ServiceMessage toMessage(Payload payload) {
        return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
      }

      private void checkMethodInvokerExist(ServiceMessage message) {
        if (!methodRegistry.containsInvoker(message.qualifier())) {
          LOGGER.error("Failed to invoke service with args[{}], No service invoker found by qualifier: {}",
              message, message.qualifier());
          throw new ServiceUnavailableException(
              "No service invoker registered at service method registry by qualifier: " + message.qualifier());
        }
      }
    });
  }
}
