package io.scalecube.services.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.HeadAndTail;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Optional;
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

  private final ServiceMessageCodec messageCodec;
  private final ServiceMethodRegistry methodRegistry;

  public RSocketServiceAcceptor(ServiceMessageCodec codec, ServiceMethodRegistry methodRegistry) {
    this.messageCodec = codec;
    this.methodRegistry = methodRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket socket) {
    LOGGER.info("Accepted rSocket: {}, connectionSetup: {}", socket, setup);
    return Mono.just(new AbstractRSocket0());
  }

  private class AbstractRSocket0 extends AbstractRSocket {

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.fromCallable(() -> toMessage(payload))
          .doOnNext(this::validateRequest)
          .flatMap(
              message -> {
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                return methodInvoker.invokeOne(message, ServiceMessageCodec::decodeData);
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
                return methodInvoker.invokeMany(message, ServiceMessageCodec::decodeData);
              })
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
                ServiceMethodInvoker methodInvoker = methodRegistry.getInvoker(message.qualifier());
                return methodInvoker.invokeBidirectional(messages, ServiceMessageCodec::decodeData);
              })
          .map(this::toPayload);
    }

    private Payload toPayload(ServiceMessage response) {
      return messageCodec.encodeAndTransform(response, ByteBufPayload::create);
    }

    private ServiceMessage toMessage(Payload payload) {
      return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
    }

    /**
     * Performs basic validation on incoming message: qualifier must be present, method invoker must
     * be present in the method registry by incoming qualifier. May throw exception
     *
     * @param message incoming message
     * @throws ServiceException in case qualfier is missing or method invoker is missing by given
     *     qualifier
     */
    private void validateRequest(ServiceMessage message) throws ServiceException {
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
  }
}
