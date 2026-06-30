package io.scalecube.services.transport.rsocket;

import static io.scalecube.services.transport.rsocket.ReferenceCountUtil.safestRelease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class RSocketImpl implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketImpl.class);

  private final Principal principal;
  private final ServiceMessageCodec messageCodec;
  private final ServiceRegistry serviceRegistry;
  private final int maxMessageSize;

  public RSocketImpl(
      Principal principal, ServiceMessageCodec messageCodec, ServiceRegistry serviceRegistry) {
    this(principal, messageCodec, serviceRegistry, 0);
  }

  public RSocketImpl(
      Principal principal,
      ServiceMessageCodec messageCodec,
      ServiceRegistry serviceRegistry,
      int maxMessageSize) {
    this.principal = principal;
    this.messageCodec = messageCodec;
    this.serviceRegistry = serviceRegistry;
    this.maxMessageSize = maxMessageSize;
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
        .map(this::toResponsePayload)
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
        .handle(this::encodeStreamPayload)
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
        .handle(this::encodeStreamPayload)
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

  /** Encodes a response payload; throws {@link MessageTooLargeException} if it crosses the limit. */
  private Payload toPayload(ServiceMessage response) {
    return messageCodec.encodeAndTransform(response, maxMessageSize, ByteBufPayload::create);
  }

  /**
   * Builds a {@code 413} business-error payload for a response that exceeded the limit. The
   * oversized payload was never fully materialized (the streaming encoder aborted), so this cannot
   * OOM the responder.
   */
  private Payload toErrorPayload(ServiceMessage response, MessageTooLargeException ex) {
    final var errorMessage = DefaultErrorMapper.INSTANCE.toMessage(response.qualifier(), ex);
    return messageCodec.encodeAndTransform(errorMessage, ByteBufPayload::create);
  }

  /** request-response: an oversized response becomes the single {@code 413} reply. */
  private Payload toResponsePayload(ServiceMessage response) {
    try {
      return toPayload(response);
    } catch (MessageTooLargeException ex) {
      return toErrorPayload(response, ex);
    }
  }

  /**
   * request-stream / request-channel: an oversized element terminates the stream with a {@code 413}
   * — the error is emitted and the stream is completed, so the responder stops producing instead of
   * encoding and sending the remaining elements.
   */
  private void encodeStreamPayload(ServiceMessage response, SynchronousSink<Payload> sink) {
    try {
      sink.next(toPayload(response));
    } catch (MessageTooLargeException ex) {
      sink.next(toErrorPayload(response, ex));
      sink.complete();
    }
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
