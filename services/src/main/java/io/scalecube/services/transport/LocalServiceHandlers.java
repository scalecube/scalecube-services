package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LocalServiceHandlers implements ServiceMessageHandler {

  private ConcurrentMap<String, LocalServiceMessageHandler> localServices = new ConcurrentHashMap<>();

  private LocalServiceHandlers() {
    // Do not instantiate
  }

  public static class Builder {
    private List<Object> services;

    public Builder services(List<Object> services) {
      this.services = services;
      return this;
    }

    public LocalServiceHandlers build() {
      return new LocalServiceHandlers(this.services);
    }
  }

  public static LocalServiceHandlers.Builder builder() {
    return new Builder();
  }

  private LocalServiceHandlers(List<Object> serviceObjects) {
    serviceObjects.forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {
        Reflect.serviceMethods(serviceInterface).forEach((key, method) -> {
          // perform vailidation
          Reflect.validateMethodOrThrow(method);
          // then keep it
          String qualifier = Reflect.qualifier(serviceInterface, method);
          localServices.put(qualifier, new LocalServiceMessageHandler(qualifier, service, method));
        });
      });
    });
  }

  public boolean contains(String qualifier) {
    return localServices.get(qualifier) != null;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    try {
      LocalServiceMessageHandler dispatcher = localServices.get(message.qualifier());
      return dispatcher.requestResponse(dispatcher.toRequest(message));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    try {
      LocalServiceMessageHandler dispatcher = localServices.get(message.qualifier());
      return dispatcher.requestStream(dispatcher.toRequest(message));
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return Flux.from(HeadAndTail.createFrom(publisher)).flatMap(pair -> {
      ServiceMessage message = pair.head();
      LocalServiceMessageHandler dispatcher = localServices.get(message.qualifier());
      Flux<ServiceMessage> publisher1 = Flux.from(pair.tail()).startWith(message);
      return dispatcher.requestChannel(publisher1.map(dispatcher::toRequest));
    });
  }
}
