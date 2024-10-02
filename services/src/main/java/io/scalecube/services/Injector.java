package io.scalecube.services;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Subscriber;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.routing.Router;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

/** Service Injector scan and injects beans to a given Microservices instance. */
final class Injector {

  private Injector() {
    // Do not instantiate
  }

  /**
   * Inject instances to the microservices instance. either Microservices or ServiceProxy. Scan all
   * local service instances and inject a service proxy.
   *
   * @param microservices microservices instance
   * @param services services set
   * @return microservices instance
   */
  public static Microservices inject(Microservices microservices, Collection<Object> services) {
    services.forEach(
        service ->
            Arrays.stream(service.getClass().getDeclaredFields())
                .forEach(field -> injectField(microservices, field, service)));
    services.forEach(service -> processAfterConstruct(microservices, service));
    services.forEach(service -> processServiceDiscoverySubscriber(microservices, service));
    return microservices;
  }

  private static void processServiceDiscoverySubscriber(
      Microservices microservices, Object service) {
    if (service instanceof CoreSubscriber) {
      final Subscriber subscriberAnnotation = service.getClass().getAnnotation(Subscriber.class);
      if (subscriberAnnotation != null
          && ServiceDiscoveryEvent.class.isAssignableFrom(subscriberAnnotation.value())) {
        //noinspection unchecked,rawtypes
        microservices.listenDiscovery().subscribe((CoreSubscriber) service);
      }
    }
  }

  private static void injectField(Microservices microservices, Field field, Object service) {
    if (field.isAnnotationPresent(Inject.class) && field.getType().equals(Microservices.class)) {
      setField(field, service, microservices);
    } else if (field.isAnnotationPresent(Inject.class) && Reflect.isService(field.getType())) {
      Inject injection = field.getAnnotation(Inject.class);
      Class<? extends Router> routerClass = injection.router();
      final ServiceCall call = microservices.call();
      if (!routerClass.isInterface()) {
        call.router(routerClass);
      }
      setField(field, service, call.api(field.getType()));
    }
  }

  private static void setField(Field field, Object object, Object value) {
    try {
      field.setAccessible(true);
      field.set(object, value);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  private static void processAfterConstruct(Microservices microservices, Object targetInstance) {
    processMethodWithAnnotation(microservices, targetInstance, AfterConstruct.class);
  }

  public static void processBeforeDestroy(Microservices microservices, Object targetInstance) {
    processMethodWithAnnotation(microservices, targetInstance, BeforeDestroy.class);
  }

  private static <A extends Annotation> void processMethodWithAnnotation(
      Microservices microservices, Object targetInstance, Class<A> annotation) {
    Method[] declaredMethods = targetInstance.getClass().getDeclaredMethods();
    Arrays.stream(declaredMethods)
        .filter(method -> method.isAnnotationPresent(annotation))
        .forEach(
            targetMethod -> {
              targetMethod.setAccessible(true);
              Object[] parameters =
                  Arrays.stream(targetMethod.getParameters())
                      .map(
                          mapper -> {
                            if (mapper.getType().equals(Microservices.class)) {
                              return microservices;
                            } else if (Reflect.isService(mapper.getType())) {
                              return microservices.call().api(mapper.getType());
                            } else {
                              return null;
                            }
                          })
                      .toArray();
              try {
                targetMethod.invoke(targetInstance, parameters);
              } catch (Exception ex) {
                throw Exceptions.propagate(ex);
              }
            });
  }
}
