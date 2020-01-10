package io.scalecube.services;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.routing.Router;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service Injector scan and injects beans to a given Microservices instance. */
final class Injector {

  private static final Logger LOGGER = LoggerFactory.getLogger(Injector.class);

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
    try {
      if (field.isAnnotationPresent(io.scalecube.services.annotations.Inject.class)
          && field.getType().equals(Microservices.class)) {
        Reflect.setField(field, service, microservices);
      } else if (field.isAnnotationPresent(io.scalecube.services.annotations.Inject.class)
          && Reflect.isService(field.getType())) {
        io.scalecube.services.annotations.Inject injection =
            field.getAnnotation(io.scalecube.services.annotations.Inject.class);
        Class<? extends Router> routerClass = injection.router();

        final ServiceCall call = microservices.call();

        if (!routerClass.isInterface()) {
          call.router(routerClass);
        }

        final Object targetProxy = call.api(field.getType());

        Reflect.setField(field, service, targetProxy);
      }
    } catch (Exception ex) {
      LOGGER.error(
          "failed to set service proxy of type: {} reason:{}",
          service.getClass().getName(),
          ex.getMessage());
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
                throw new RuntimeException(ex);
              }
            });
  }
}
