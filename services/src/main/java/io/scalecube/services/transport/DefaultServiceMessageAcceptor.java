package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.server.api.ServiceMessageAcceptor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServiceMessageAcceptor implements ServiceMessageAcceptor {

  private final LocalServiceDispatchers localServiceDispatchers;
  private final ServiceMessageDataCodec messageDataCodec;

  public DefaultServiceMessageAcceptor(LocalServiceDispatchers localServiceDispatchers) {
    this.localServiceDispatchers = localServiceDispatchers;
    this.messageDataCodec = new ServiceMessageDataCodec();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException("requestChannel is not implemented");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = messageDataCodec.decode(request, dispatcher.requestType());
    return dispatcher.requestStream(message).map(messageDataCodec::encode);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = messageDataCodec.decode(request, dispatcher.requestType());
    return dispatcher.requestResponse(message).map(messageDataCodec::encode);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<Void> fireAndForget(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = localServiceDispatchers.getDispatcher(request.qualifier());
    ServiceMessage message = messageDataCodec.decode(request, dispatcher.requestType());
    return dispatcher.fireAndForget(message);
  }

}
