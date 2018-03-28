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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class ServiceStreams {

  private final StreamProcessors.ServerStreamProcessors server;

  public ServiceStreams(StreamProcessors.ServerStreamProcessors server) {
    this.server = server;
  }

  public List<ServiceMethodSubscription> createSubscriptions(Object serviceObject) {

    List<AbstractMap.SimpleEntry<Qualifier, Method>> methods = Reflect.serviceInterfaces(serviceObject).stream()
        .flatMap(serviceInterface -> Reflect.serviceMethods(serviceInterface).entrySet().stream().map(entry -> {
          String namespace = Reflect.serviceName(serviceInterface);
          String action = entry.getKey();
          Method method = entry.getValue();
          return new AbstractMap.SimpleEntry<>(new Qualifier(namespace, action), method);
        }))
        .collect(Collectors.toList());

    return Collections.unmodifiableList(methods.stream()
        .map(entry -> createServiceMethodSubscription(serviceObject, entry.getKey(), entry.getValue()))
        .collect(Collectors.toList()));
  }

  private ServiceMethodSubscription createServiceMethodSubscription(Object serviceObject,
      Qualifier qualifier, Method method) {

    ServiceMethodSubscription subscription = new ServiceMethodSubscription(server, qualifier, method, serviceObject);

    Class<?> returnType = method.getReturnType();
    if (returnType == CompletableFuture.class) {
      return subscription.toCompletableFuture();
    } else if (returnType == Observable.class) {
      return subscription.toObservable();
    } else if (returnType == Void.class) {
      return subscription.toVoid();
    } else if (returnType == Subscriber.class && containStreamProcessor(method.getParameters())) {
      return subscription.requestStreamToResponseStream();
    } else {
      throw new IllegalArgumentException();
    }
  }

  private boolean containStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }
}
