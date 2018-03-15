package io.scalecube.services;

import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Message;

import com.google.common.reflect.Reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ServiceProxyFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  private ServiceRegistry serviceRegistry;

  private ServiceCall dispatcher;

  private Microservices microservices;

  /**
   * Constructor for this proxy factory.
   * 
   * @param microservices {@link Microservices} instance
   */
  public ServiceProxyFactory(Microservices microservices) {
    this.microservices = microservices;
    this.serviceRegistry = microservices.serviceRegistry();
  }

  /**
   * createProxy creates a java generic proxy instance by a given service interface.
   * 
   * @param <T> service interface type
   * @param serviceInterface the service interface, api, of the service.
   * @param routerType the type of routing method class to be used.
   * @param metrics optional performance metrics.
   * @return newly created service proxy object.
   */
  public <T> T createProxy(Class<T> serviceInterface, final Class<? extends Router> routerType,
      Duration timeout, Metrics metrics) {

    ServiceDefinition serviceDefinition = serviceRegistry.registerInterface(serviceInterface);
    dispatcher = microservices.dispatcher().router(routerType).timeout(timeout).create();

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Metrics.mark(serviceInterface, metrics, method, "request");

        Object data = method.getParameterCount() != 0 ? args[0] : null;
        final Message reqMsg = getRequestMessage(serviceDefinition, method, data);
        if (method.getReturnType().equals(Observable.class)) {
          if (Reflect.parameterizedReturnType(method).equals(Message.class)) {
            return dispatcher.listen(reqMsg);
          } else {
            return dispatcher.listen(reqMsg).map(message -> message.data());
          }
        } else {
          return toReturnValue(method,
              dispatcher.invoke(reqMsg));
        }
      }

      private Message getRequestMessage(ServiceDefinition serviceDefinition, Method method, Object data) {
        if (data instanceof Message) {
          return Messages.builder().request(serviceDefinition.serviceName(),
              method.getName())
              .data(((Message) data).data())
              .build();
        } else {
          return Messages.builder().request(serviceDefinition.serviceName(),
              method.getName())
              .data(data)
              .build();
        }
      }

      private CompletableFuture<T> toReturnValue(final Method method, final CompletableFuture<Message> reuslt) {
        final CompletableFuture<T> future = new CompletableFuture<>();

        if (method.getReturnType().equals(Void.TYPE)) {
          return (CompletableFuture<T>) CompletableFuture.completedFuture(Void.TYPE);

        } else if (method.getReturnType().equals(CompletableFuture.class)) {
          reuslt.whenComplete((value, ex) -> {
            if (ex == null) {
              Metrics.mark(serviceInterface, metrics, method, "response");
              if (!Reflect.parameterizedReturnType(method).equals(Message.class)) {
                future.complete(value.data());
              } else {
                future.complete((T) value);
              }
            } else {
              Metrics.mark(serviceInterface, metrics, method, "error");
              LOGGER.error("return value is exception: {}", ex);
              future.completeExceptionally(ex);
            }
          });
          return future;
        } else {
          LOGGER.error("return value is not supported type.");
          future.completeExceptionally(new UnsupportedOperationException());
        }
        return future;
      }
    });
  }
}
