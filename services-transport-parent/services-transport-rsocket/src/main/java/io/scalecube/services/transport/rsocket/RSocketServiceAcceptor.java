package io.scalecube.services.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RSocket service acceptor. Implementation of {@link SocketAcceptor}. See for details and supported
 * methods -- {@link AbstractRSocket0}.
 */
public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final Consumer<Object> requestReleaser = ReferenceCountUtil::safestRelease;

  private final ServiceMessageCodec messageCodec;
  private final ServiceMethodRegistry methodRegistry;

  public RSocketServiceAcceptor(ServiceMessageCodec codec, ServiceMethodRegistry methodRegistry) {
    this.messageCodec = codec;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rsocket) {
    return Mono.<RSocket>deferWithContext(
            context -> {
              LOGGER.debug("Accepted rsocket: {}, connectionSetup: {}", rsocket, setup);
              return Mono.just(new AbstractRSocket0());
            })
        .subscriberContext(context -> context);
  }

  private class AbstractRSocket0 extends AbstractRSocket {

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.fromCallable(() -> toMessage(payload))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                validateMethodInvoker(methodInvoker, message);
                return methodInvoker.invokeOne(message);
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
                return methodInvoker.invokeMany(message);
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
                  validateMethodInvoker(methodInvoker, message);
                  return methodInvoker.invokeBidirectional(messages);
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
        applyRequestReleaser(message);
        LOGGER.error("[qualifier is null] Invocation failed for {}", message);
        throw new BadRequestException("Qualifier is null");
      }
    }

    private void validateMethodInvoker(ServiceMethodInvoker methodInvoker, ServiceMessage message) {
      if (methodInvoker == null) {
        applyRequestReleaser(message);
        LOGGER.error("[no service invoker found] Invocation failed for {}", message);
        throw new ServiceUnavailableException("No service invoker found");
      }
    }

    private void applyRequestReleaser(ServiceMessage request) {
      if (request.data() != null) {
        requestReleaser.accept(request.data());
      }
    }
  }
}
