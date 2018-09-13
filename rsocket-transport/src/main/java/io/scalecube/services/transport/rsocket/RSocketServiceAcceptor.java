package io.scalecube.services.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.HeadAndTail;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ReferenceCountUtil;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodRegistry;
import java.util.Optional;
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

    return Mono.just(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.just(payload)
                .map(this::toMessage)
                .doOnNext(this::validateRequest)
                .flatMap(
                    message ->
                        methodRegistry
                            .getInvoker(message.qualifier())
                            .invokeOne(message, ServiceMessageCodec::decodeData))
                .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)))
                .map(this::toPayload);
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            return Flux.just(payload)
                .map(this::toMessage)
                .doOnNext(this::validateRequest)
                .flatMap(
                    message ->
                        methodRegistry
                            .getInvoker(message.qualifier())
                            .invokeMany(message, ServiceMessageCodec::decodeData))
                .onErrorResume(t -> Flux.just(ExceptionProcessor.toMessage(t)))
                .map(this::toPayload);
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(HeadAndTail.createFrom(Flux.from(payloads).map(this::toMessage)))
                .flatMap(
                    pair -> {
                      ServiceMessage message = pair.head();
                      validateRequest(message);
                      Flux<ServiceMessage> messages = Flux.from(pair.tail()).startWith(message);
                      return methodRegistry
                          .getInvoker(message.qualifier())
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

          private void validateRequest(ServiceMessage message) {
            if (message.qualifier() == null) {
              Optional.ofNullable(message.data())
                  .ifPresent(ReferenceCountUtil::safestRelease); // release message data if any
              LOGGER.error("Failed to invoke service with msg={}: qualifier is null", message);
              throw new BadRequestException("Qualifier is null in service msg request: " + message);
            }

            if (!methodRegistry.containsInvoker(message.qualifier())) {
              Optional.ofNullable(message.data())
                  .ifPresent(ReferenceCountUtil::safestRelease); // release message data if any
              LOGGER.error(
                  "Failed to invoke service with msg={}: no service invoker found by qualifier={}",
                  message,
                  message.qualifier());
              throw new ServiceUnavailableException(
                  "No service invoker found by qualifier=" + message.qualifier());
            }
          }
        });
  }
}
