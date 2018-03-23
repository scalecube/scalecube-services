package io.scalecube.services.streams;

import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class ServiceSubscriptionBuilder {

  private final StreamProcessors.ServerStreamProcessors server;
  private final Qualifier qualifier;
  private final Method method;
  private final Type parameterizedReturnType;
  private final Parameter[] parameters;
  private final Object serviceObject;

  public ServiceSubscriptionBuilder(
      StreamProcessors.ServerStreamProcessors server,
      Qualifier qualifier,
      Method method,
      Type parameterizedReturnType,
      Parameter[] parameters,
      Object serviceObject) {
    this.server = server;
    this.qualifier = qualifier;
    this.method = method;
    this.parameterizedReturnType = parameterizedReturnType;
    this.parameters = parameters;
    this.serviceObject = serviceObject;
  }

  public Subscription singleRequestToCompletableFuture() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          Object invoke = method.invoke(serviceObject, message);
          // noinspection unchecked
          ((CompletableFuture<StreamMessage>) invoke).whenComplete((reponse, error) -> {
            if (reponse != null) {
              observer.onNext(reponse);
              observer.onCompleted();
            }
            if (error != null) {
              observer.onError(error);
            }
          });
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
  }

  public Subscription singleRequestToObservable() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          // noinspection unchecked
          Observable<StreamMessage> invoke = (Observable<StreamMessage>) method.invoke(serviceObject, message);
          invoke.subscribe(observer::onNext, observer::onError, observer::onCompleted);
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
  }

  public Subscription singleRequestToVoid() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          method.invoke(serviceObject, message);
          observer.onCompleted();
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
  }

  public Subscription requestStreamToResponseStream() {
    return listenStreamProcessor(streamProcessor -> {
      try {
        // noinspection unchecked
        return ((Subscriber<StreamMessage>) method.invoke(serviceObject, streamProcessor));
      } catch (Throwable error) {
        return new SubscriberAdapter();
      }
    });
  }

  private Subscription listenStreamProcessor(Function<StreamProcessor, Subscriber<StreamMessage>> factory) {
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
}
