package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.ServerStreamProcessors;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.List;
import java.util.stream.Collectors;

public final class ServiceStreams {

  private final ServerStreamProcessors server;

  public ServiceStreams(ServerStreamProcessors server) {
    this.server = server;
  }

  /**
   * create service subscriptions to a given service object.
   * 
   * @param serviceObject to introspect and create stream subscriptions.
   * @return list of stream subscription found for object.
   */
  public List<ServiceMethodSubscription> createSubscriptions(Object serviceObject) {

    List<AbstractMap.SimpleEntry<Qualifier, Method>> methods = Reflect.serviceInterfaces(serviceObject).stream()
        .flatMap(serviceInterface -> Reflect.serviceMethods(serviceInterface).entrySet().stream().map(entry -> {
          String namespace = Reflect.serviceName(serviceInterface);
          String action = entry.getKey();
          Method method = entry.getValue();
          return new AbstractMap.SimpleEntry<>(new Qualifier(namespace, action), method);
        }))
        .collect(Collectors.toList());

    return methods.stream()
        .map(entry -> ServiceMethodSubscription.create(server, entry.getKey(), entry.getValue(), serviceObject))
        .collect(Collectors.toList());
  }
}
