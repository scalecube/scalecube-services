package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.MessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.api.CommunicationMode;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.dispatchers.FireAndForgetInvoker;
import io.scalecube.services.transport.dispatchers.RequestChannelDispatcher;
import io.scalecube.services.transport.dispatchers.RequestResponseDispatcher;
import io.scalecube.services.transport.dispatchers.RequestStreamDispatcher;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceDispatchers implements ServerMessageAcceptor {

  @SuppressWarnings("rawtypes")
  private ConcurrentMap<String, ServiceMethodDispatcher> localServices = new ConcurrentHashMap<>();
  private List<Object> services;
  private List<ServiceMessageDataCodec> codecs;

  private LocalServiceDispatchers() {
    // noop. use create().
  }

  public static class Builder {
    private Object[] services;
    private MessageCodec[] messageCodecs;

    public Builder services(Object[] services) {
      this.services = services;
      return this;
    }

    public Builder codecs(MessageCodec... messageCodec) {
      this.messageCodecs = messageCodec;
      return this;
    }

    public LocalServiceDispatchers build() {
      return new LocalServiceDispatchers(this.messageCodecs, this.services);
    }
  }

  public static LocalServiceDispatchers.Builder builder() {
    return new Builder();
  }

  private LocalServiceDispatchers(MessageCodec[] messageCodecs, Object[] serviceObjects) {
    this.codecs = Arrays.stream(messageCodecs).map(codec -> (ServiceMessageDataCodec) codec)
        .collect(Collectors.toList());
    this.services = Arrays.asList(serviceObjects);

    this.services().forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {

        Reflect.serviceMethods(serviceInterface).forEach((key, method) -> this.codecs.forEach(codec -> {
          Optional<CommunicationMode> communicationMode = CommunicationMode.of(method);
          String qualifier = Reflect.qualifier(serviceInterface, method);
          if (communicationMode.get().equals(CommunicationMode.REQUEST_ONE)) {
            this.register(qualifier, new RequestResponseDispatcher(qualifier, service, method, codec));

          } else if (communicationMode.get().equals(CommunicationMode.REQUEST_STREAM)) {
            this.register(qualifier, new RequestChannelDispatcher(qualifier, service, method, codec));

          } else if (communicationMode.get().equals(CommunicationMode.ONE_WAY)) {
            this.register(qualifier, new FireAndForgetInvoker(qualifier, service, method, codec));

          } else if (communicationMode.get().equals(CommunicationMode.REQUEST_MANY)) {
            this.register(qualifier, new RequestStreamDispatcher(qualifier, service, method, codec));
          }
        }));
      });
    });
  }


  public boolean contains(String qualifier) {
    return localServices.get(qualifier) != null;
  }

  public Collection<Object> services() {
    return Collections.unmodifiableCollection(this.services);
  }

  public List<? extends ServiceMessageDataCodec> codecs() {
    return this.codecs;
  }

  public Publisher dispatchLocalService(ServiceMessage request) {
    // noinspection unchecked
    return localServices.get(request.qualifier()).invoke(request);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    ServiceMethodDispatcher<Publisher<ServiceMessage>> handler = null;
    return Flux.from(handler.invoke(request));
  }


  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = get(request.qualifier());
    ServiceMessageDataCodec codec = dispatcher.getCodec();

    return Flux.from(dispatcher.invoke(request)).map(resp -> {
      ServiceMessage msg = (ServiceMessage) resp;
      return codec.encodeData(msg);
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    ServiceMethodDispatcher dispatcher = get(request.qualifier());
    ServiceMessageDataCodec codec = dispatcher.getCodec();

    return Mono.from(dispatcher.invoke(request)).map(resp -> {
      ServiceMessage msg = (ServiceMessage) resp;
      return codec.encodeData(msg);
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<Void> fireAndForget(ServiceMessage request) {
    return get(request.qualifier())
        .invoke(request);
  }


  private ServiceMethodDispatcher get(String qualifier) {
    return localServices.get(qualifier);
  }

  private void register(final String qualifier, ServiceMethodDispatcher handler) {
    localServices.put(qualifier, handler);
  }

}
