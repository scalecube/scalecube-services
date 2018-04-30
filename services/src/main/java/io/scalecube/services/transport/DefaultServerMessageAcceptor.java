package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

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
  public Flux<ServiceMessage> requestChannel(final Flux<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    try {
      ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
      ServiceMessageCodec codec = getCodec(request);
      ServiceMessage message = codec.decodeData(request, dispatcher.requestType());
      return Flux
          .from(dispatcher.invoke(message))
          .map(resp -> codec.encodeData((ServiceMessage) resp));
    } catch (Throwable t) {
      return Flux.error(t);
    }

  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    try {
      ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
      ServiceMessageCodec codec = getCodec(request);
      ServiceMessage withDecodedData = codec.decodeData(request, dispatcher.requestType());
      return Mono
          .from(dispatcher.invoke(withDecodedData))
          .map(resp -> codec.encodeData((ServiceMessage) resp));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<ServiceMessage> fireAndForget(ServiceMessage request) {
    try {
      ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
      ServiceMessageCodec codec = getCodec(request);
      ServiceMessage message = codec.decodeData(request, dispatcher.requestType());
      return Mono.from(dispatcher.invoke(message));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  private ServiceMessageCodec getCodec(ServiceMessage request) {
    return this.codecs.get("application/json");
  }

}
