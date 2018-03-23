package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class ServiceStreams {

  public static void main(String[] args) {
    StreamProcessors.ServerStreamProcessors server = StreamProcessors.server().build();

    Object serviceObject = null;

    List<AbstractMap.SimpleEntry<Qualifier, Method>> methods = Reflect.serviceInterfaces(serviceObject).stream()
        .flatMap(serviceInterface -> Reflect.serviceMethods(serviceInterface).entrySet().stream().map(entry -> {
          String namespace = Reflect.serviceName(serviceInterface);
          String action = entry.getKey();
          Method method = entry.getValue();
          return new AbstractMap.SimpleEntry<>(new Qualifier(namespace, action), method);
        }))
        .collect(Collectors.toList());

    List<Subscription> subscriptions = methods.stream()
        .map(entry -> {
          Qualifier qualifier = entry.getKey();
          Method method = entry.getValue();
          Class<?> returnType = method.getReturnType();
          Type parameterizedReturnType = Reflect.parameterizedReturnType(method);
          Parameter[] parameters = method.getParameters();
          boolean parameterContainStreamProcessor =
              parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;

          ServiceSubscriptionBuilder stubBuilder = new ServiceSubscriptionBuilder(server, qualifier, method,
              parameterizedReturnType, parameters, serviceObject);

          Subscription subscription;
          if (returnType == CompletableFuture.class) {
            subscription = stubBuilder.singleRequestToCompletableFuture();
          } else if (returnType == Observable.class) {
            subscription = stubBuilder.singleRequestToObservable();
          } else if (returnType == Void.class) {
            subscription = stubBuilder.singleRequestToVoid();
          } else if (returnType == Subscriber.class && parameterContainStreamProcessor) {
            subscription = stubBuilder.requestStreamToResponseStream();
          } else {
            throw new IllegalArgumentException();
          }
          return subscription;
        }).collect(Collectors.toList());
  }
}
