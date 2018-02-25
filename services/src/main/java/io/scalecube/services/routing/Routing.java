package io.scalecube.services.routing;

import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.transport.Message;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

@FunctionalInterface
public interface Routing {

  /**
   * Routing is a selection mechanism to find a service that will be able to reply a request.
   * 
   * @param registry - the {@link ServiceRegistry registry} to search in.
   * @param request - a request to be passed to the instance.
   * @returns service instance in the registry - if the request message is applicable to one or
   *          {@link Optional#empty()}.
   */
  default Optional<ServiceInstance> route(ServiceRegistry registry, Message request) {
    return routes(registry, request).stream().findFirst();
  }

  /**
   * Routing is a selection mechanism to find a service that will be able to reply a request.
   * 
   * @param registry - the {@link ServiceRegistry registry} to search in.
   * @param request - a request to be passed to the instance.
   * @returns all applicable routes.
   */
  Collection<ServiceInstance> routes(ServiceRegistry registry, Message request);

  /**
   * Most implementations of Routing would like to test that a method exists in the {@link ServiceInstance service
   * instance}. <br />
   * This {@link BiPredicate} tests a service instance and a message.
   * 
   * @returns true if the instance {@link ServiceInstance#methodExists has} the {@link ServiceHeaders.METHOD method}
   *          which requested in the message {@link Message#header(String) header}
   * 
   */
  public static final BiPredicate<ServiceInstance, Message> methodExists =
      (instance, request) -> instance.methodExists(request.header(ServiceHeaders.METHOD));


  public static Stream<ServiceInstance> serviceLookup(ServiceRegistry serviceRegistry, Message request) {
    Predicate<ServiceInstance> hasMethod = instance -> methodExists.test(instance, request);
    return serviceRegistry.serviceLookup(request.header(ServiceHeaders.SERVICE_REQUEST)).stream().filter(hasMethod);
  }

  /**
   * Build a routing object from a router class, which in time would.
   * 
   * @param router the router to build from
   * @return a routing that delegates calls to an instance of the class
   */
  public static Routing fromRouter(Class<? extends Router> router) {
    AtomicReference<Router> routerReference = new AtomicReference<>(null);
    Constructor<? extends Router> constructor;
    try {
      constructor = router.getConstructor(ServiceRegistry.class);
    } catch (NoSuchMethodException | SecurityException ignoredException1) {
      throw new RuntimeException(ignoredException1);
    }
    return new Routing() {
      @Override
      public Optional<ServiceInstance> route(ServiceRegistry registry, Message request) {
        try {
          if (routerReference.get() == null) {
            routerReference.weakCompareAndSet(null, constructor.newInstance(registry));
          }
          return routerReference.get().route(request);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | SecurityException ignoredException) {
          ignoredException.printStackTrace();
          return Optional.empty();
        }
      }

      @Override
      public Collection<ServiceInstance> routes(ServiceRegistry registry, Message request) {
        try {
          if (routerReference.get() == null) {
            routerReference.weakCompareAndSet(null, constructor.newInstance(registry));
          }
          return routerReference.get().routes(request);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | SecurityException ignoredException) {
          ignoredException.printStackTrace();
          return Collections.emptyList();
        }
      }
    };
  }
}
