package io.scalecube.services.transport.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Collection;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final ConnectionSetupCodec connectionSetupCodec;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final Authenticator<Object> authenticator;
  private final ServiceMethodRegistry methodRegistry;

  /**
   * Constructor.
   *
   * @param connectionSetupCodec connectionSetupCodec
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param authenticator authenticator
   * @param methodRegistry methodRegistry
   */
  public RSocketServiceAcceptor(
      ConnectionSetupCodec connectionSetupCodec,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      Authenticator<Object> authenticator,
      ServiceMethodRegistry methodRegistry) {
    this.connectionSetupCodec = connectionSetupCodec;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.authenticator = authenticator;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket rsocket) {
    LOGGER.info("[rsocket][accept][{}] Setup: {}", rsocket, setupPayload);

    return Mono.justOrEmpty(decodeConnectionSetup(setupPayload.data()))
        .flatMap(connectionSetup -> authenticate(rsocket, connectionSetup))
        .flatMap(
            authData ->
                Mono.fromCallable(
                    () ->
                        new RSocketImpl(
                            authData,
                            new ServiceMessageCodec(headersCodec, dataCodecs),
                            methodRegistry)))
        .switchIfEmpty(
            Mono.fromCallable(
                () ->
                    new RSocketImpl(
                        null /*authData*/,
                        new ServiceMessageCodec(headersCodec, dataCodecs),
                        methodRegistry)))
        .cast(RSocket.class);
  }

  private ConnectionSetup decodeConnectionSetup(ByteBuf byteBuf) {
    if (byteBuf.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, false /*releaseOnClose*/)) {
        return connectionSetupCodec.decode(stream);
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(byteBuf); // release byteBuf
        throw new MessageCodecException("Failed to decode ConnectionSetup", ex);
      }
    }
    return null;
  }

  private Mono<Object> authenticate(RSocket rsocket, ConnectionSetup connectionSetup) {
    if (authenticator == null || connectionSetup == null || !connectionSetup.hasCredentials()) {
      return Mono.empty();
    }
    return authenticator
        .apply(connectionSetup.credentials())
        .doOnSuccess(obj -> LOGGER.debug("[rsocket][authenticate][{}] Authenticated", rsocket))
        .doOnError(
            ex ->
                LOGGER.error(
                    "[rsocket][authenticate][{}] Exception occurred: {}", rsocket, ex.toString()))
        .onErrorMap(this::toUnauthorizedException);
  }

  private UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException) {
      ServiceException e = (ServiceException) th;
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  private static class RSocketImpl implements RSocket {

    private final Object authData;
    private final ServiceMessageCodec messageCodec;
    private final ServiceMethodRegistry methodRegistry;

    private RSocketImpl(
        @Nullable Object authData,
        ServiceMessageCodec messageCodec,
        ServiceMethodRegistry methodRegistry) {
      this.authData = authData;
      this.messageCodec = messageCodec;
      this.methodRegistry = methodRegistry;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.deferContextual(context -> Mono.just(toMessage(payload)))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker
                    .invokeOne(message)
                    .doOnNext(response -> releaseRequestOnError(message, response));
              })
          .map(this::toPayload)
          .doOnError(ex -> LOGGER.error("[requestResponse] Exception occurred: {}", ex.toString()))
          .contextWrite(this::enhanceContextWithAuthData);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Mono.deferContextual(context -> Mono.just(toMessage(payload)))
          .doOnNext(this::validateRequest)
          .flatMapMany(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker
                    .invokeMany(message)
                    .doOnNext(response -> releaseRequestOnError(message, response));
              })
          .map(this::toPayload)
          .doOnError(ex -> LOGGER.error("[requestStream] Exception occurred: {}", ex.toString()))
          .contextWrite(this::enhanceContextWithAuthData);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.deferContextual(context -> Flux.from(payloads))
          .map(this::toMessage)
          .switchOnFirst(
              (first, messages) -> {
                if (first.hasValue()) {
                  ServiceMessage message = first.get();
                  validateRequest(message);
                  ServiceMethodInvoker methodInvoker =
                      methodRegistry.getInvoker(message.qualifier());
                  return methodInvoker
                      .invokeBidirectional(messages)
                      .doOnNext(response -> releaseRequestOnError(message, response));
                }
                return messages;
              })
          .map(this::toPayload)
          .doOnError(ex -> LOGGER.error("[requestChannel] Exception occurred: {}", ex.toString()))
          .contextWrite(this::enhanceContextWithAuthData);
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

    private Context enhanceContextWithAuthData(Context context) {
      return authData != null ? context.put(Authenticator.AUTH_CONTEXT_KEY, authData) : context;
    }

    private void validateRequest(ServiceMessage message) throws ServiceException {
      if (message.qualifier() == null) {
        releaseRequest(message);
        LOGGER.error("[qualifier is null] Invocation failed for {}", message);
        throw new BadRequestException("Qualifier is null");
      }
    }

    private void validateMethodInvoker(ServiceMethodInvoker methodInvoker, ServiceMessage message) {
      if (methodInvoker == null) {
        releaseRequest(message);
        LOGGER.error("[no service invoker found] Invocation failed for {}", message);
        throw new ServiceUnavailableException("No service invoker found");
      }
    }

    private void releaseRequest(ServiceMessage request) {
      ReferenceCountUtil.safestRelease(request.data());
    }

    private void releaseRequestOnError(ServiceMessage request, ServiceMessage response) {
      if (response.isError()) {
        releaseRequest(request);
      }
    }
  }
}
