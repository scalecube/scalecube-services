package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServerMessageAcceptor implements ServerMessageAcceptor {

  private final LocalServiceDispatchers localServiceDispatchers;
  private final ServiceMessageDataCodec codec;

  public DefaultServerMessageAcceptor(LocalServiceDispatchers localServiceDispatchers) {
    this.localServiceDispatchers = localServiceDispatchers;
    this.codec = new ServiceMessageDataCodec();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Flux<ServiceMessage>) Flux.from(dispatcher.invoke(message))
        .map(resp -> codec.encodeData((ServiceMessage) resp)))
            .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Mono<ServiceMessage>) Mono.from(dispatcher.invoke(message))
        .map(resp -> codec.encodeData((ServiceMessage) resp)))
            .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> fireAndForget(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Mono<ServiceMessage>) Mono.from(dispatcher.invoke(message)))
        .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

}
