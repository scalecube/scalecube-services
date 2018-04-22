package io.scalecube.services.transport;

import io.scalecube.rsockets.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    return handlers.get(request.qualifier())
        .invoke(request);
  }


  private LocalServiceInvoker(List<? extends ServiceMessageCodec> codecs, Object... serviceObjects) {
    this.codec = codecs;
    this.services = serviceObjects;

    Arrays.asList(serviceObjects).forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {

        Reflect.serviceMethods(serviceInterface).entrySet().forEach(entry -> {
          codecs.forEach(codec -> {
            Optional<CommunicationMode> communicationMode = CommunicationMode.of(entry.getValue());
            if (communicationMode.get().equals(CommunicationMode.REQUEST_ONE)) {

              this.register(Reflect.qualifier(serviceInterface, entry.getValue()),
                  new RequestResponseInvoker(service, entry.getValue(), codec));

            } else if (communicationMode.get().equals(CommunicationMode.REQUEST_STREAM)) {
              this.register(Reflect.qualifier(serviceInterface, entry.getValue()),
                  new RequestChannelInvoker(service, entry.getValue(), codec));
            }
          });
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
