package io.scalecube.services.transport;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.dispatchers.FireAndForgetDispatcher;
import io.scalecube.services.transport.dispatchers.RequestResponseDispatcher;
import io.scalecube.services.transport.dispatchers.RequestStreamDispatcher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceDispatchers {

  @SuppressWarnings("rawtypes")
  private ConcurrentMap<String, ServiceMethodDispatcher> localServices = new ConcurrentHashMap<>();

  private List<Object> services;

  private LocalServiceDispatchers() {
    // noop. use create().
  }

  public static class Builder {
    private List<Object> services;

    public Builder services(List<Object> services) {
      this.services = services;
      return this;
    }

    public LocalServiceDispatchers build() {
      return new LocalServiceDispatchers(this.services);
    }
  }

  public static LocalServiceDispatchers.Builder builder() {
    return new Builder();
  }

  private LocalServiceDispatchers(List<Object> serviceObjects) {
    this.services = Collections.singletonList(serviceObjects);

    serviceObjects.forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {
        Reflect.serviceMethods(serviceInterface).forEach((key, method) -> {
          String qualifier = Reflect.qualifier(serviceInterface, method);
          CommunicationMode mode = Reflect.communicationMode(method);
          if (mode == CommunicationMode.FIRE_AND_FORGET) {
            register(qualifier, new FireAndForgetDispatcher(qualifier, service, method));
          } else if (mode == CommunicationMode.REQUEST_RESPONSE) {
            register(qualifier, new RequestResponseDispatcher(qualifier, service, method));
          } else if (mode == CommunicationMode.REQUEST_STREAM) {
            register(qualifier, new RequestStreamDispatcher(qualifier, service, method));
          } else {
            throw new IllegalArgumentException("REQUEST_CHANNEL mode is not supported: " + method);
          }
        });
      });
    });
  }

  public boolean contains(String qualifier) {
    return localServices.get(qualifier) != null;
  }

  public Collection<Object> services() {
    return Collections.unmodifiableCollection(this.services);
  }

  public ServiceMethodDispatcher getDispatcher(String qualifier) {
    return localServices.get(qualifier);
  }

  private void register(final String qualifier, ServiceMethodDispatcher handler) {
    localServices.put(qualifier, handler);
  }

}
