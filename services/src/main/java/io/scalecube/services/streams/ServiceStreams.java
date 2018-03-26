package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;

import rx.Observable;
import rx.Subscriber;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class ServiceStreams {

  final StreamProcessors.ServerStreamProcessors server;

  private ServiceStreams(Builder builder) {
    this.server = builder.server;
  }

  public static class Builder {
    StreamProcessors.ServerStreamProcessors server = StreamProcessors.server();

    public Builder server(StreamProcessors.ServerStreamProcessors server) {
      this.server = server;
      return this;
    }

    public ServiceStreams build() {
      server.build();
      return new ServiceStreams(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<ServiceMethodSubscription> from(Object serviceObject) {

    List<AbstractMap.SimpleEntry<Qualifier, Method>> methods = Reflect.serviceInterfaces(serviceObject).stream()
        .flatMap(serviceInterface -> Reflect.serviceMethods(serviceInterface).entrySet().stream().map(entry -> {
          String namespace = Reflect.serviceName(serviceInterface);
          String action = entry.getKey();
          Method method = entry.getValue();
          return new AbstractMap.SimpleEntry<>(new Qualifier(namespace, action), method);
        }))
        .collect(Collectors.toList());

    return methods.stream()
        .map(entry -> {
          Qualifier qualifier = entry.getKey();
          Method method = entry.getValue();
          return createServiceMethodSubscription(serviceObject, qualifier, method);
        }).collect(Collectors.toList());

  }

  private ServiceMethodSubscription createServiceMethodSubscription(Object serviceObject,
      Qualifier qualifier, Method method) {

    ServiceMethodSubscription sub = new ServiceMethodSubscription(this.server,
        qualifier,
        method, serviceObject);

    Class<?> returnType = method.getReturnType();
    if (returnType == CompletableFuture.class) {
      return sub.toCompletableFuture();
    } else if (returnType == Observable.class) {
      return sub.toObservable();
    } else if (returnType == Void.class) {
      return sub.toVoid();
    } else if (returnType == Subscriber.class && containStreamProcessor(method.getParameters())) {
      return sub.requestStreamToResponseStream();
    } else {
      throw new IllegalArgumentException();
    }
  }

  static private boolean containStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }

}
