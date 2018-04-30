package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServerMessageAcceptor implements ServerMessageAcceptor {

  private final LocalServiceDispatchers localServiceDispatchers;
  private final Map<String, ? extends ServiceMessageCodec> codecs;

  public DefaultServerMessageAcceptor(LocalServiceDispatchers localServiceDispatchers,
      Map<String, ? extends ServiceMessageCodec> codecs) {
    this.localServiceDispatchers = localServiceDispatchers;
    this.codecs = codecs;
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
    ServiceMessageCodec codec = getCodec(request);
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Flux<ServiceMessage>) Flux.from(dispatcher.invoke(message))
        .map(resp -> codec.encodeData((ServiceMessage) resp)))
            .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessageCodec codec = getCodec(request);
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Mono<ServiceMessage>) Mono.from(dispatcher.invoke(message))
        .map(resp -> codec.encodeData((ServiceMessage) resp)))
            .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> fireAndForget(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessageCodec codec = getCodec(request);
    ServiceMessage message = codec.decodeData(request, dispatcher.requestType());

    return ((Mono<ServiceMessage>) Mono.from(dispatcher.invoke(message)))
        .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }

  private ServiceMessageCodec getCodec(ServiceMessage request) {
    return this.codecs.get("application/json");
  }

}
