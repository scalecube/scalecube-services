package io.scalecube.services.transport.server.api;

import io.scalecube.services.Reflect;
import io.scalecube.services.transport.Qualifier;

import org.reactivestreams.Subscriber;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class ServiceMethodAdapter implements Disposable {
  private final ServiceTransport server;
  private final Qualifier qualifier;
  private final Method method;
  private final Object serviceObject;
  private final Class<?> requestType;
  private Disposable subsciption;


  public static class Builder {
    /**
     * Create a new method subscription server that accept stream messages and invokes a service method.
     * 
     * @param server stream listening and accepting network traffic.
     * @param qualifier on which stream is accepted.
     * @param method to invoke in case of stream message.
     * @param serviceObject instance to invoke.
     * @return new service method subscription.
     */
    private static ServiceMethodAdapter create(ServiceTransport server, Qualifier qualifier, Method method,
        Object serviceObject) {

      ServiceMethodAdapter subscription =
          new ServiceMethodAdapter(server, qualifier, method, serviceObject);

      Class<?> returnType = method.getReturnType();
      if (returnType == CompletableFuture.class) {
        return subscription.requestReply();
      } else if (returnType == Flux.class) {
        return subscription.requestStream();
      } else if (Void.TYPE.equals(returnType)) {
        return subscription.fireAndFoget();
      } else if (returnType == Subscriber.class) {
        return subscription.bidirectional();
      } else {
        throw new IllegalArgumentException();
      }
    }
    
    /**
     * create service subscriptions to a given service object.
     * 
     * @param serviceObject to introspect and create stream subscriptions.
     * @return list of stream subscription found for object.
     */
    public List<ServiceMethodAdapter> create(ServiceTransport server, Object serviceObject) {

      List<AbstractMap.SimpleEntry<Qualifier, Method>> methods = Reflect.serviceInterfaces(serviceObject).stream()
          .flatMap(serviceInterface -> Reflect.serviceMethods(serviceInterface).entrySet().stream().map(entry -> {
            String namespace = Reflect.serviceName(serviceInterface);
            String action = entry.getKey();
            Method method = entry.getValue();
            return new AbstractMap.SimpleEntry<>(new Qualifier(namespace, action), method);
          }))
          .collect(Collectors.toList());

      return methods.stream()
          .map(entry -> ServiceMethodAdapter.builder().create(server, entry.getKey(), entry.getValue(), serviceObject))
          .collect(Collectors.toList());
    }
  }

  public static Builder builder() {
    return new Builder();
  }
  
  private ServiceMethodAdapter(
      ServiceTransport server,
      Qualifier qualifier,
      Method method,
      Object serviceObject) {
    this.server = server;
    this.qualifier = qualifier;
    this.method = method;
    this.serviceObject = serviceObject;
    this.requestType = Reflect.requestType(method);
  }

  @Override
  public void dispose() {
    if (subsciption != null) {
      subsciption.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return Objects.isNull(subsciption) || subsciption.isDisposed();
  }

  protected ServiceMethodAdapter requestReply() {
    throw new UnsupportedOperationException("requestReply was not implemented for this instance");
  }

  protected ServiceMethodAdapter requestStream() {
    throw new UnsupportedOperationException("requestStream was not implemented for this instance");
  }

  protected ServiceMethodAdapter fireAndFoget() {
    throw new UnsupportedOperationException("fireAndFoget was not implemented for this instance");
  }

  protected ServiceMethodAdapter bidirectional() {
    throw new UnsupportedOperationException("bidirectional was not implemented for this instance");
  }

 

}
