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

public class ServiceProxytFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxytFactory.class);

  private final ServiceProcessor serviceProcessor;
  private ConcurrentMap<String, ServiceDefinition> serviceDefinitions;
  private RouterFactory routerFactory;

  public ServiceProxytFactory(ServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
    this.routerFactory = new RouterFactory(serviceRegistry);
    this.serviceProcessor = serviceProcessor;
  }

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
                .qualifier(serviceInstance.qualifier())
                .build(),
                Optional.of(serviceDefinition));

          } else {
            LOGGER.error(
                "Faild  to invoke service, No reachable member with such service definition [{}], args [{}]",
                serviceDefinition, args);
            CompletableFuture<T> f = new CompletableFuture<T>();
            f.completeExceptionally(
                new IllegalStateException("No reachable member with such service: " + method.getName()));
            if (method.getReturnType().isAssignableFrom(CompletableFuture.class)) {
              return f;
            } else {
              return null;
            }
          }

        } catch (RuntimeException e) {
          LOGGER.error(
              "Faild  to invoke service, No reachable member with such service method [{}], args [{}], error [{}]",
              method, args, e);
          CompletableFuture<T> f = new CompletableFuture<T>();
          f.completeExceptionally(
              new IllegalStateException("No reachable member with such service: " + method.getName(), e));
          return f;
        }
      }
    });
  }
}
