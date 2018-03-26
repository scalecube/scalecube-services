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
    
    public Builder server(StreamProcessors.ServerStreamProcessors server){
      this.server = server;
      return this;
    }
       
    public ServiceStreams build(){
      server.build();
      return new ServiceStreams(this);
    }
  }
  
  public static Builder builder(){
    return new Builder();
  } 
  
  public List<Subscription> from(Object serviceObject){
    
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
          
          Parameter[] parameters = method.getParameters();
         
          ServiceMethodSubscription pattern = new ServiceMethodSubscription(this.server, qualifier, method, serviceObject);

          Subscription subscription;
          
          Class<?> returnType = method.getReturnType();
          if (returnType == CompletableFuture.class) {
            subscription = pattern.toCompletableFuture();
          } else if (returnType == Observable.class) {
            subscription = pattern.toObservable();
          } else if (returnType == Void.class) {
            subscription = pattern.toVoid();
          } else if (returnType == Subscriber.class && containStreamProcessor(parameters)) {
            subscription = pattern.requestStreamToResponseStream();
          } else {
            throw new IllegalArgumentException();
          }
          return subscription;
        }).collect(Collectors.toList());
    
    return subscriptions;
  }

  static private boolean containStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }
  
}
