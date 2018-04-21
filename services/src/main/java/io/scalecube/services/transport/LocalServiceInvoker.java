package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceInvoker implements ServerMessageAcceptor {

  @SuppressWarnings("rawtypes")
  private ConcurrentMap<String, ServiceMethodInvoker> handlers = new ConcurrentHashMap<>();
  private Object[] services;
  private List<? extends ServiceMessageCodec> codec;

  public static LocalServiceInvoker create(List<? extends ServiceMessageCodec> codecs, Object... serviceObjects) {
    return new LocalServiceInvoker(codecs, serviceObjects);
  }

  public Collection<Object> services() {
    return Collections.unmodifiableCollection(Arrays.asList(this.services));
  }

  public List<? extends ServiceMessageCodec> codec() {
    return this.codec;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    ServiceMethodInvoker<Publisher<ServiceMessage>> handler = null;
    return handler.invoke(request);
  }


  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    return handlers.get(request.qualifier()).invoke(request);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    return handlers.get(request.qualifier())
        .invoke(request);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<Void> fireAndForget(ServiceMessage request) {
    return Mono.from(handlers.get(request.qualifier())
        .invoke(request))
        .map(msg -> null);
  }


  private LocalServiceInvoker(List<? extends ServiceMessageCodec> codecs, Object... serviceObjects) {
    this.codec = codecs;
    this.services = serviceObjects;

    Arrays.asList(serviceObjects).forEach(service -> {
      Reflect.serviceInterfaces(service).stream().map(ServiceInterface -> {

        return Reflect.serviceMethods(ServiceInterface)
            .entrySet().stream().map(entry -> {
              codecs.forEach(codec -> {
                {
                  if (Reflect.exchangeTypeOf(entry.getValue()).equals(ExchangeType.REQUEST_RESPONSE)) {
                    this.register(Reflect.qualifier(ServiceInterface, entry.getValue()),
                        new RequestResponseInvoker(service, entry.getValue(), codec));

                  } else if (Reflect.exchangeTypeOf(entry.getValue()).equals(ExchangeType.REQUEST_CHANNEL)) {
                    this.register(Reflect.qualifier(ServiceInterface, entry.getValue()),
                        new RequestChannelInvoker(service, entry.getValue(), codec));
                  }
                }
              });
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
