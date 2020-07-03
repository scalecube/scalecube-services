package io.scalecube.services.transport.rsocket;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
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

public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final ConnectionSetupCodec connectionSetupCodec;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final Authenticator authenticator;
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
      Authenticator authenticator,
      ServiceMethodRegistry methodRegistry) {
    this.connectionSetupCodec = connectionSetupCodec;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.authenticator = authenticator;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket rsocket) {
    LOGGER.info("Accepted rsocket: {}, setupPayload: {}", rsocket, setupPayload);

    return Mono.just(new RSocketImpl(messageCodec, methodRegistry));
  }

  private static class RSocketImpl implements RSocket {

    private final ServiceMessageCodec messageCodec;
    private final ServiceMethodRegistry methodRegistry;

    private RSocketImpl(ServiceMessageCodec messageCodec, ServiceMethodRegistry methodRegistry) {
      this.messageCodec = messageCodec;
      this.methodRegistry = methodRegistry;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.fromCallable(() -> toMessage(payload))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker
                    .invokeOne(message)
                    .doOnNext(response -> releaseRequestOnError(message, response));
              })
          .map(this::toPayload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Mono.fromCallable(() -> toMessage(payload))
          .doOnNext(this::validateRequest)
          .flatMapMany(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker
                    .invokeMany(message)
                    .doOnNext(response -> releaseRequestOnError(message, response));
              })
          .map(this::toPayload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads)
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
          .map(this::toPayload);
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
