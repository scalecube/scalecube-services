package io.scalecube.services;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.Futures;

import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.transport.Message;

public class ServiceProxytFactory {

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

          ServiceInstance serviceInstance = findInstance(method,routerType);

          if (serviceInstance != null) {
            return serviceInstance.invoke(Message.builder()
                .data(args[0])
                .qualifier(serviceInstance.qualifier())
                .build()
                ,method.getReturnType());
          } else {
            return Futures
                .immediateFailedFuture(new IllegalStateException("No reachable member with such service"));
          }
        } catch (RuntimeException e) {
          e.printStackTrace();
          return Futures
              .immediateFailedFuture(new IllegalStateException("No reachable member with such service"));
        }
      }

      private ServiceInstance findInstance(Method method,final Class<? extends Router> routerType) {
        ServiceDefinition serviceDefinition = serviceDefinitions.get(method.getName());
        Router router = routerFactory.getRouter(routerType);
        return router.route(serviceDefinition);
      }
    });
  }
}
