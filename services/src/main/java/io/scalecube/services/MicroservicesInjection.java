package io.scalecube.services;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.routing.Router;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service Injector scan and injects beans to a given Microservices instance. */
public final class MicroservicesInjection {

  private static final Logger LOGGER = LoggerFactory.getLogger(MicroservicesInjection.class);

  private MicroservicesInjection() {
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
    } else if (field.isAnnotationPresent(Inject.class) && isService(field.getType())) {
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

  private static void processAfterConstruct(Microservices microservices, Object targetInstance) {
    Method[] declaredMethods = targetInstance.getClass().getDeclaredMethods();
    Arrays.stream(declaredMethods)
        .filter(method -> method.isAnnotationPresent(AfterConstruct.class))
        .forEach(
            afterConstructMethod -> {
              try {
                afterConstructMethod.setAccessible(true);
                Object[] parameters =
                    Arrays.stream(afterConstructMethod.getParameters())
                        .map(
                            mapper -> {
                              if (mapper.getType().equals(Microservices.class)) {
                                return microservices;
                              } else if (isService(mapper.getType())) {
                                return microservices.call().api(mapper.getType());
                              } else {
                                return null;
                              }
                            })
                        .toArray();
                afterConstructMethod.invoke(targetInstance, parameters);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  private static boolean isService(Class<?> type) {
    return type.isAnnotationPresent(Service.class);
  }

  private static void setField(Field field, Object object, Object value) {
    try {
      field.setAccessible(true);
      field.set(object, value);
    } catch (Exception ex) {
      LOGGER.error(
          "failed to set service proxy of type: {} reason:{}",
          object.getClass().getName(),
          ex.getMessage());
    }
  }
}
