package io.scalecube.services;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.routing.Router;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
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
    return microservices;
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
      final Object targetProxy = call.api(field.getType());
      setField(field, service, targetProxy);
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
              try {
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
                targetMethod.invoke(targetInstance, parameters);
              } catch (Exception ex) {
                throw Exceptions.propagate(ex);
              }
            });
  }
}
