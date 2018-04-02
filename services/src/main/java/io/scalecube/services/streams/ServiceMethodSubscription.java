package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.streams.StreamProcessors.ServerStreamProcessors;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class ServiceMethodSubscription implements Subscription {

  private final StreamProcessors.ServerStreamProcessors server;
  private final Qualifier qualifier;
  private final Method method;
  private final Object serviceObject;
  private Subscription subsciption;

  private ServiceMethodSubscription(
      StreamProcessors.ServerStreamProcessors server,
      Qualifier qualifier,
      Method method,
      Object serviceObject) {
    this.server = server;
    this.qualifier = qualifier;
    this.method = method;
    this.serviceObject = serviceObject;
  }

  public static ServiceMethodSubscription create(ServerStreamProcessors server, Qualifier qualifier, Method method,
      Object serviceObject) {

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

  public ServiceMethodSubscription toCompletableFuture() {
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          // noinspection unchecked
          CompletableFuture<StreamMessage> result = invoke(message);
          result.whenComplete((reponse, error) -> {
            if (error == null) {
              observer.onNext(reponse);
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

  public ServiceMethodSubscription toObservable() {
    this.subsciption = accept(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage request) {
        try {
          Observable<StreamMessage> result = invoke(request);
          result.subscribe(observer);
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
    return this;
  }

  public ServiceMethodSubscription toVoid() {
    this.subsciption = accept(streamProcessor -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          invoke(message);
          streamProcessor.onCompleted();
        } catch (Throwable error) {
          streamProcessor.onError(error);
        }
      }
    });
    return this;
  }

  public ServiceMethodSubscription requestStreamToResponseStream() {
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
    return Reflect.invoke(serviceObject, method, message);
  }

  @SuppressWarnings("unchecked")
  private Subscriber<StreamMessage> invoke(final StreamProcessor streamProcessor) throws Exception {
    // noinspection unchecked
    return (Subscriber<StreamMessage>) method.invoke(serviceObject, streamProcessor);
  }

  private Subscription accept(Function<StreamProcessor, Subscriber<StreamMessage>> factory) {
    return server.listen().subscribe(streamProcessor -> { // => got new stream processor
      // listen for stream messages with qualifier filter
      streamProcessor.listen()
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

  private static boolean containStreamProcessor(Parameter[] parameters) {
    return parameters.length > 0 && parameters[0].getType() == StreamProcessor.class;
  }


}
