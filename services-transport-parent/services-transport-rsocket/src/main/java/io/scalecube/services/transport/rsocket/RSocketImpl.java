package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.methods.MethodInfo;
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

  RSocketImpl(Object principal, ServiceMessageCodec messageCodec, ServiceRegistry serviceRegistry) {
    this.principal = principal;
    this.messageCodec = messageCodec;
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
            () -> {
              final var message = toMessage(payload);
              validateRequest(message);

              final var methodInvoker = serviceRegistry.lookupInvoker(message);
              validateMethodInvoker(methodInvoker, message);

              return methodInvoker
                  .invokeOne(message)
                  .doOnNext(response -> releaseRequestOnError(message, response))
                  .contextWrite(context -> setupContext(message));
            })
        .map(this::toPayload)
        .doOnError(ex -> LOGGER.error("[requestResponse][error] cause: {}", ex.toString()));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.defer(
            () -> {
              final var message = toMessage(payload);
              validateRequest(message);

              final var methodInvoker = serviceRegistry.lookupInvoker(message);
              validateMethodInvoker(methodInvoker, message);

              return methodInvoker
                  .invokeMany(message)
                  .doOnNext(response -> releaseRequestOnError(message, response))
                  .contextWrite(context -> setupContext(message));
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
                validateRequest(message);

                final var methodInvoker = serviceRegistry.lookupInvoker(message);
                validateMethodInvoker(methodInvoker, message);

                return methodInvoker
                    .invokeBidirectional(messages)
                    .doOnNext(response -> releaseRequestOnError(message, response))
                    .contextWrite(context -> setupContext(message));
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

  private Context setupContext(ServiceMessage message) {
    return Context.of(
        RequestContext.class,
        RequestContext.builder().headers(message.headers()).principal(principal).build());
  }

  private static void validateRequest(ServiceMessage message) throws ServiceException {
    if (message == null) {
      throw new BadRequestException("Message is null, invocation failed");
    }
    if (message.qualifier() == null) {
      releaseRequest(message);
      throw new BadRequestException("Qualifier is null, invocation failed for " + message);
    }
  }

  private static void validateMethodInvoker(
      ServiceMethodInvoker methodInvoker, ServiceMessage message) {
    if (methodInvoker == null) {
      releaseRequest(message);
      LOGGER.error("No service invoker found, invocation failed for {}", message);
      throw new ServiceUnavailableException("No service invoker found");
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

  private Mono<Object> doAuth(MethodInfo methodInfo) {
    return RequestContext.deferContextual()
        .flatMap(
            context -> {
              if (!methodInfo.isSecured()) {
                return Mono.just(RequestContext.NULL_PRINCIPAL);
              }

              if (authenticator == null) {
                if (context.hasKey(AUTH_CONTEXT_KEY)) {
                  return Mono.just(context.get(AUTH_CONTEXT_KEY));
                } else {
                  throw new UnauthorizedException("Authentication failed");
                }
              }

              return authenticator
                  .apply(context.headers())
                  .switchIfEmpty(Mono.just(RequestContext.NULL_PRINCIPAL))
                  .onErrorMap(RSocketImpl::toUnauthorizedException);
            });
  }

  private static UnauthorizedException toUnauthorizedException(Throwable ex) {
    if (ex instanceof ServiceException e) {
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(ex);
    }
  }
}
