package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.streams.codec.StreamMessageDataCodecImpl;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class ServiceMethodSubscription implements Subscription {
  private final ServerStreamProcessors server;
  private final Qualifier qualifier;
  private final Method method;
  private final Object serviceObject;
  private final Type requestType;
  private Subscription subsciption;
  private StreamMessageDataCodec codec = new StreamMessageDataCodecImpl();

  private ServiceMethodSubscription(
      ServerStreamProcessors server,
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
  public static ServiceMethodSubscription create(ServerStreamProcessors server, Qualifier qualifier, Method method,
      Object serviceObject) {

    ServiceMethodSubscription subscription = new ServiceMethodSubscription(server, qualifier, method, serviceObject);

    Class<?> returnType = method.getReturnType();
    if (returnType == CompletableFuture.class) {
      return subscription.toCompletableFuture();
    } else if (returnType == Observable.class) {
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
  public void unsubscribe() {
    if (subsciption != null) {
      subsciption.unsubscribe();
    }
  }

  @Override
  public boolean isUnsubscribed() {
    return Objects.isNull(subsciption) || subsciption.isUnsubscribed();
  }

  private ServiceMethodSubscription toCompletableFuture() {
    // Class<?> reqPayloadType = reqType();
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          // noinspection unchecked
          CompletableFuture<Object> result = invoke(message);
          result.whenComplete((response, error) -> {
            if (error == null) {
              observer.onNext(codec.encodeData(StreamMessage.from(message).data(response).build()));
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

  private ServiceMethodSubscription toObservable() {
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage request) {
        try {
          Observable<StreamMessage> result = invoke(request);
          result.map(message -> codec.encodeData(message)).subscribe(observer);
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
    return this;
  }

  private ServiceMethodSubscription toVoid() {
    this.subsciption = accept(streamProcessor -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          invoke(message);
          streamProcessor.onNext(StreamMessage.from(message).data(null).build());
          streamProcessor.onCompleted();
        } catch (Throwable error) {
          streamProcessor.onError(error);
        }
      }
    });
    return this;
  }

  private ServiceMethodSubscription toBidirectional() {
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

  private <T> T invoke(StreamMessage message) throws Exception {
    if (requestType.equals(Void.TYPE)) {
      return Reflect.invoke(serviceObject, method, message);
    } else {
      if (StreamMessage.class.equals(requestType)) {
        return Reflect.invoke(serviceObject, method, message);
      } else {
        return Reflect.invoke(serviceObject, method,
            codec.decodeData(message, Class.forName(requestType.getTypeName())));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Subscriber<StreamMessage> invoke(final StreamProcessor streamProcessor) throws Exception {
    // noinspection unchecked
    return (Subscriber<StreamMessage>) method.invoke(serviceObject, streamProcessor);
  }

  private Subscription accept(Function<StreamProcessor, Subscriber<StreamMessage>> factory) {
    return server.listen().subscribe(streamProcessor -> {
      // listen for stream messages with qualifier filter
      // noinspection unchecked
      ((StreamProcessor<StreamMessage, StreamMessage>) streamProcessor).listen()
          .filter(message -> qualifier.asString().equalsIgnoreCase(message.qualifier()))
          .subscribe(factory.apply(streamProcessor));
    });
  }

  private static class SubscriberAdapter extends Subscriber<StreamMessage> {

    private SubscriberAdapter() {}

    @Override
    public void onNext(StreamMessage message) {
      // no-op
    }

    @Override
    public void onCompleted() {
      // no-op
    }

    @Override
    public void onError(Throwable error) {
      // no-op
    }
  }

  private static boolean containsStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }
}
