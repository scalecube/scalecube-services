package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessageHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LocalServiceHandlers {

  private ConcurrentMap<String, ServiceMessageHandler> localServices = new ConcurrentHashMap<>();

  private List<Object> services;

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
    this.services = Collections.singletonList(serviceObjects);

    serviceObjects.forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {
        Reflect.serviceMethods(serviceInterface).forEach((key, method) -> {
          // perform vailidation
          Reflect.validateMethodOrThrow(method);
          // then keep it
          String qualifier = Reflect.qualifier(serviceInterface, method);
          put(qualifier, new LocalServiceMessageHandler(qualifier, service, method));
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

  public ServiceMessageHandler get(String qualifier) {
    return localServices.get(qualifier);
  }

  private void put(String qualifier, ServiceMessageHandler handler) {
    localServices.put(qualifier, handler);
  }
}
