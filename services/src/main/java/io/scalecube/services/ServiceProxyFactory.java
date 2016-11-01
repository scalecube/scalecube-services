package io.scalecube.services;

import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.transport.Message;

import com.google.common.reflect.Reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

public class ServiceProxyFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  private final ServiceProcessor serviceProcessor;
  private ConcurrentMap<String, ServiceDefinition> serviceDefinitions;
  private RouterFactory routerFactory;

  public ServiceProxyFactory(ServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
    this.routerFactory = new RouterFactory(serviceRegistry);
    this.serviceProcessor = serviceProcessor;
  }

  /**
   * createProxy creates a java generic proxy instance by a given service interface.
   * @param serviceInterface the service interface, api, of the service.
   * @param routerType the type of routing method class to be used.
   * @return newly created service proxy object.
   */
  public <T> T createProxy(Class<T> serviceInterface, final Class<? extends Router> routerType) {

    this.serviceDefinitions = serviceProcessor.introspectServiceInterface(serviceInterface);

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {

          ServiceDefinition serviceDefinition = serviceDefinitions.get(method.getName());

          Router router = routerFactory.getRouter(routerType);
          ServiceInstance serviceInstance = router.route(serviceDefinition);

          if (serviceInstance != null) {
            return serviceInstance.invoke(Message.builder()
                .data(args[0])
                .qualifier(serviceInstance.serviceName())
                .build(),
                Optional.of(serviceDefinition));

          } else {
            LOGGER.error(
                "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
                serviceDefinition, args);
            CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(
                new IllegalStateException("No reachable member with such service: " + method.getName()));
            if (method.getReturnType().isAssignableFrom(CompletableFuture.class)) {
              return future;
            } else {
              return null;
            }
          }

        } catch (RuntimeException e) {
          LOGGER.error(
              "Failed  to invoke service, No reachable member with such service method [{}], args [{}], error [{}]",
              method, args, e);
          CompletableFuture<T> future = new CompletableFuture<>();
          future.completeExceptionally(
              new IllegalStateException("No reachable member with such service: " + method.getName(), e));
          return future;
        }
      }
    });
  }
}
