package io.scalecube.services.transport.rsocket;

import static io.scalecube.services.transport.rsocket.ReferenceCountUtil.safestRelease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketImpl implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketImpl.class);

  private final Principal principal;
  private final ServiceMessageCodec messageCodec;
  private final ServiceRegistry serviceRegistry;

  public RSocketImpl(
      Principal principal, ServiceMessageCodec messageCodec, ServiceRegistry serviceRegistry) {
    this.principal = principal;
    this.messageCodec = messageCodec;
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
            () -> {
              final var message = toMessage(payload);
              return lookupInvoker(message)
                  .flatMap(
                      methodInvoker ->
                          methodInvoker
                              .invokeOne(message)
                              .doOnNext(response -> releaseOnError(message, response))
                              .contextWrite(requestContext(message)));
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestResponse] Exception occurred", ex));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.defer(
            () -> {
              final var message = toMessage(payload);
              return lookupInvoker(message)
                  .flatMapMany(
                      methodInvoker ->
                          methodInvoker
                              .invokeMany(message)
                              .doOnNext(response -> releaseOnError(message, response))
                              .contextWrite(requestContext(message)));
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestStream] Exception occurred", ex));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return Flux.from(payloads)
        .map(this::toMessage)
        .switchOnFirst(
            (first, messages) -> {
              if (first.hasValue()) {
                final var message = first.get();
                return lookupInvoker(message)
                    .flatMapMany(
                        methodInvoker ->
                            methodInvoker
                                .invokeBidirectional(messages)
                                .doOnNext(response -> releaseOnError(message, response))
                                .contextWrite(requestContext(message)));
              }
              return messages;
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestChannel] Exception occurred", ex));
  }

  private Mono<ServiceMethodInvoker> lookupInvoker(ServiceMessage message) {
    return Mono.fromCallable(
            () -> {
              final var methodInvoker = serviceRegistry.lookupInvoker(message);
              if (methodInvoker == null) {
                throw new ServiceUnavailableException("No service invoker found");
              }
              return methodInvoker;
            })
        .doOnError(ex -> safestRelease(message.data()));
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

  private RequestContext requestContext(ServiceMessage message) {
    return new RequestContext().headers(message.headers()).principal(principal);
  }

  private static void releaseOnError(ServiceMessage request, ServiceMessage response) {
    if (response.isError()) {
      safestRelease(request.data());
    }
  }
}
