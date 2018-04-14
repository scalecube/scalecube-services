package io.scalecube.services.transport.server.api;

import io.scalecube.services.Reflect;
import io.scalecube.services.transport.Qualifier;
import io.scalecube.services.transport.ServiceMessage;
import io.scalecube.services.transport.server.api.ServiceChannel;

import io.netty.channel.ServerChannel;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public abstract class ServiceMethodAdapter implements Disposable {
  private final ServerTransport server;
  private final Qualifier qualifier;
  private final Method method;
  private final Object serviceObject;
  private final Class<?> requestType;
  private Disposable subsciption;

  private ServiceMethodAdapter(
      ServerTransport server,
      Qualifier qualifier,
      Method method,
      Object serviceObject) {
    this.server = server;
    this.qualifier = qualifier;
    this.method = method;
    this.serviceObject = serviceObject;
    this.requestType = Reflect.requestType(method);
  }

  /**
   * Create a new method subscription server that accept stream messages and invokes a service method.
   * 
   * @param server stream listening and accepting network traffic.
   * @param qualifier on which stream is accepted.
   * @param method to invoke in case of stream message.
   * @param serviceObject instance to invoke.
   * @return new service method subscription.
   */
  public static ServiceMethodAdapter create(ServerTransport server, Qualifier qualifier, Method method,
      Object serviceObject) {

    ServiceMethodAdapter subscription = new ServiceMethodAdapter(server, qualifier, method, serviceObject);

    Class<?> returnType = method.getReturnType();
    if (returnType == CompletableFuture.class) {
      return subscription.toCompletableFuture();
    } else if (returnType == Flux.class) {
      return subscription.toObservable();
    } else if (Void.TYPE.equals(returnType)) {
      return subscription.toVoid();
    } else if (returnType == Subscriber.class && containsStreamProcessor(method.getParameters())) {
      return subscription.toBidirectional();
    } else {
      throw new IllegalArgumentException();
    }

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

  private abstract ServiceMethodAdapter toCompletableFuture() {
    // Class<?> reqPayloadType = reqType();
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(ServiceMessage message) {
        try {
          // noinspection unchecked
          CompletableFuture<Object> result = invoke(message);
          result.whenComplete((response, error) -> {
            if (error == null) {
              observer.onNext(codec.encodeData(ServiceMessage.from(message).data(response).build()));
              observer.onCompleted();
            } else {
              observer.onError(error);
            }
          });
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
    return this;
  }

  private ServiceMethodAdapter toObservable() {
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(ServiceMessage request) {
        try {
          Observable<ServiceMessage> result = invoke(request);
          result.map(message -> codec.encodeData(message)).subscribe(observer);
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
    return this;
  }

  private ServiceMethodAdapter toVoid() {
    this.subsciption = accept(streamProcessor -> new SubscriberAdapter() {
      @Override
      public void onNext(ServiceMessage message) {
        try {
          invoke(message);
          streamProcessor.onNext(ServiceMessage.from(message).data(null).build());
          streamProcessor.onCompleted();
        } catch (Throwable error) {
          streamProcessor.onError(error);
        }
      }
    });
    return this;
  }

  private ServiceMethodAdapter toBidirectional() {
    this.subsciption = accept(streamProcessor -> {
      try {
        // noinspection unchecked
        return invoke(streamProcessor);
      } catch (Throwable error) {
        streamProcessor.onError(error);
        return new SubscriberAdapter();
      }
    });
    return this;
  }

  private <T> T invoke(ServiceMessage message) throws Exception {
    if (requestType.equals(Void.TYPE)) {
      return Reflect.invoke(serviceObject, method, message);
    } else {
      if (ServiceMessage.class.equals(requestType)) {
        return Reflect.invoke(serviceObject, method, message);
      } else {
        return Reflect.invoke(serviceObject, method, codec.decodeData(message, requestType));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Subscriber<ServiceMessage> invoke(final StreamProcessor streamProcessor) throws Exception {
    // noinspection unchecked
    return (Subscriber<ServiceMessage>) method.invoke(serviceObject, streamProcessor);
  }

  private Subscription accept(Function<StreamProcessor, Subscriber<ServiceMessage>> factory) {
    return server.listen().subscribe(streamProcessor -> {
      // listen for stream messages with qualifier filter
      // noinspection unchecked
      ((StreamProcessor<ServiceMessage, ServiceMessage>) streamProcessor).listen()
          .filter(message -> qualifier.asString().equalsIgnoreCase(message.qualifier()))
          .subscribe(factory.apply(streamProcessor));
    });
  }

  private static boolean containsStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }
}
