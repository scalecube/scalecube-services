package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceInvoker implements ServerMessageAcceptor {

  @SuppressWarnings("rawtypes")
  private ConcurrentMap<String, ServiceMethodInvoker> handlers = new ConcurrentHashMap<>();
  private Object[] services;
  private ServiceMessageCodec codec;

  public static LocalServiceInvoker create(ServiceMessageCodec codec, Object... serviceObjects) {
    return new LocalServiceInvoker(codec, serviceObjects);
  }

  public Collection<Object> services() {
    return Collections.unmodifiableCollection(Arrays.asList(this.services));
  }
  public ServiceMessageCodec codec() {
    return this.codec;
  }
  @Override
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    ServiceMethodInvoker<Publisher<ServiceMessage>> handler = null;
    return handler.invoke(request);
  }


  @Override
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    return handlers.get(request.qualifier()).invoke(request);
  }

  @Override
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    return handlers.get(request.qualifier())
        .invoke(request);
  }

  @Override
  public Publisher<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(handlers.get(request.qualifier())
        .invoke(request))
        .map(msg -> null);
  }


  private LocalServiceInvoker(ServiceMessageCodec codec, Object... serviceObjects) {
    this.codec = codec;
    this.services = serviceObjects;

    Arrays.asList(serviceObjects).forEach(service -> {
      Reflect.serviceInterfaces(service).stream().map(serviceIinterface -> {

        return Reflect.serviceMethods(serviceIinterface)
            .entrySet().stream().map(entry -> {

              if (Reflect.exchangeTypeOf(entry.getValue()).equals(ExchangeType.REQUEST_RESPONSE)) {
                this.register(Reflect.methodName(entry.getValue()),
                    new RequestResponseInvoker(service, entry.getValue(), codec));
              } else if (Reflect.exchangeTypeOf(entry.getValue()).equals(ExchangeType.REQUEST_CHANNEL)) {
                this.register(Reflect.methodName(entry.getValue()),
                    new RequestChannelInvoker(service, entry.getValue(), codec));
              }
              return null;
            });
      });
    });
  }


  private void register(final String qualifier, ServiceMethodInvoker handler) {
    handlers.put(qualifier, handler);
  }

  private LocalServiceInvoker() {
    // noop. use create().
  }
}
