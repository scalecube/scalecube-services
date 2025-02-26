package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class RSocketImpl implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketImpl.class);

  private final Object principal;
  private final ServiceMessageCodec messageCodec;
  private final ServiceRegistry serviceRegistry;

  public RSocketImpl(
      Object principal, ServiceMessageCodec messageCodec, ServiceRegistry serviceRegistry) {
    this.principal = principal;
    this.messageCodec = messageCodec;
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
            () -> {
              final var message = toMessage(payload);
              final var methodInvoker = serviceRegistry.lookupInvoker(message);
              validateRequest(methodInvoker, message);
              return methodInvoker
                  .invokeOne(message)
                  .doOnNext(response -> releaseRequestOnError(message, response))
                  .contextWrite(requestContext(message));
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestResponse][error] cause: {}", ex.toString()));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.defer(
            () -> {
              final var message = toMessage(payload);
              final var methodInvoker = serviceRegistry.lookupInvoker(message);
              validateRequest(methodInvoker, message);
              return methodInvoker
                  .invokeMany(message)
                  .doOnNext(response -> releaseRequestOnError(message, response))
                  .contextWrite(requestContext(message));
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestStream][error] cause: {}", ex.toString()));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return Flux.from(payloads)
        .map(this::toMessage)
        .switchOnFirst(
            (first, messages) -> {
              if (first.hasValue()) {
                final var message = first.get();
                final var methodInvoker = serviceRegistry.lookupInvoker(message);
                validateRequest(methodInvoker, message);
                return methodInvoker
                    .invokeBidirectional(messages)
                    .doOnNext(response -> releaseRequestOnError(message, response))
                    .contextWrite(requestContext(message));
              }
              return messages;
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestChannel][error] cause: {}", ex.toString()));
  }

  private Payload toPayload(ServiceMessage response) {
    return messageCodec.encodeAndTransform(response, ByteBufPayload::create);
  }

  private ServiceMessage toMessage(Payload payload) {
    try {
      return messageCodec.decode(payload.sliceData().retain(), payload.sliceMetadata().retain());
    } finally {
      payload.release();
    }
  }

  private Context requestContext(ServiceMessage message) {
    return RequestContext.builder()
        .headers(message.headers())
        .principal(principal)
        .build()
        .toContext();
  }

  private static void validateRequest(ServiceMethodInvoker methodInvoker, ServiceMessage message) {
    if (methodInvoker == null) {
      releaseRequest(message);
      throw new ServiceUnavailableException("No service invoker found");
    }
    if (message.qualifier() == null) {
      releaseRequest(message);
      throw new BadRequestException("Qualifier is null, invocation failed for " + message);
    }
  }

  private static void releaseRequest(ServiceMessage request) {
    ReferenceCountUtil.safestRelease(request.data());
  }

  private static void releaseRequestOnError(ServiceMessage request, ServiceMessage response) {
    if (response.isError()) {
      releaseRequest(request);
    }
  }
}
