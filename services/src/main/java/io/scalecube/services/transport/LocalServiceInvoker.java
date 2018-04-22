package io.scalecube.services.transport;

import io.scalecube.rsockets.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.MessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceInvoker implements ServerMessageAcceptor {

  @SuppressWarnings("rawtypes")
  private ConcurrentMap<String, ServiceMethodInvoker> localServices = new ConcurrentHashMap<>();
  private Object[] services;
  private List<? extends ServiceMessageDataCodec> codecs;

  public static LocalServiceInvoker create(List<? extends MessageCodec> codecs, Object... serviceObjects) {
    return new LocalServiceInvoker(codecs, serviceObjects);
  }

  public Collection<Object> services() {
    return Collections.unmodifiableCollection(Arrays.asList(this.services));
  }

  public List<? extends ServiceMessageDataCodec> codec() {
    return this.codecs;
  }

  public boolean contains(String qualifier) {
    return localServices.get(qualifier)!=null;
  }
  
  private ServiceMethodInvoker get(String qualifier) {
    return localServices.get(qualifier);
  }

  public Publisher invokeLocal(String qualifier, Object request) {
    return localServices.get(qualifier).invoke(request);
  }
  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestChannel(final Publisher<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    ServiceMethodInvoker<Publisher<ServiceMessage>> handler = null;
    return Flux.from(handler.invoke(request));
  }


  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestStream(ServiceMessage request) {
    ServiceMethodInvoker handler = get(request.qualifier());
    ServiceMessageDataCodec codec = handler.getCodec();

    return Flux.from(handler.invoke(request)).map(resp -> {
      ServiceMessage msg = (ServiceMessage) resp;
      return codec.encodeData(msg);
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public Publisher<ServiceMessage> requestResponse(ServiceMessage request) {
    ServiceMethodInvoker handler = get(request.qualifier());
    ServiceMessageDataCodec codec = handler.getCodec();

    return Mono.from(handler.invoke(request)).map(resp -> {
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


  private LocalServiceInvoker(List<? extends ServiceMessageDataCodec> codecs, Object... serviceObjects) {
    this.codecs = codecs;
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

            } else if (communicationMode.get().equals(CommunicationMode.ONE_WAY)) {
              this.register(Reflect.qualifier(serviceInterface, entry.getValue()),
                  new FireAndForgetInvoker(service, entry.getValue(), codec));

            } else if (communicationMode.get().equals(CommunicationMode.REQUEST_MANY)) {
              this.register(Reflect.qualifier(serviceInterface, entry.getValue()),
                  new RequestStreamInvoker(service, entry.getValue(), codec));
            }
          });
        });
      });
    });
  }


  private void register(final String qualifier, ServiceMethodInvoker handler) {
    localServices.put(qualifier, handler);
  }

  private LocalServiceInvoker() {
    // noop. use create().
  }
}
