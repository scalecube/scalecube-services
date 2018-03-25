package io.scalecube.services.streams;

import io.scalecube.services.Reflect;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.transport.Message;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class ServiceStreamMethodInvoker {

  private final StreamProcessors.ServerStreamProcessors server;
  private final Qualifier qualifier;
  private final Method method;
  private final Object serviceObject;

  public ServiceStreamMethodInvoker(
      StreamProcessors.ServerStreamProcessors server,
      Qualifier qualifier,
      Method method,
      Object serviceObject) {
    this.server = server;
    this.qualifier = qualifier;
    this.method = method;
    this.serviceObject = serviceObject;
  }

  public Subscription requestToCompletableFuture() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
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
  }

  public Subscription requestToObservable() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage request) {
        try {
          // noinspection unchecked
          Observable<StreamMessage> result = invoke(request);
          result.subscribe(observer::onNext, observer::onError, observer::onCompleted);
        } catch (Throwable error) {
          observer.onError(error);
        }
      }
    });
  }

  public Subscription requestToVoid() {
    return listenStreamProcessor(observer -> new SubscriberAdapter() {
      @Override
      public void onNext(StreamMessage message) {
        try {
          invoke(message);
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
        Subscriber<StreamMessage> result = invoke(streamProcessor);
        return result;
      } catch (Throwable error) {
        return new SubscriberAdapter();
      }
    });
  }

  private <T> T invoke(StreamMessage message) throws Exception {
    return Reflect.invoke(serviceObject, method, message);
  }
  
  private Subscriber<StreamMessage> invoke(final StreamProcessor streamProcessor)
      throws Exception {
    return (Subscriber<StreamMessage>) method.invoke(serviceObject, streamProcessor);
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
