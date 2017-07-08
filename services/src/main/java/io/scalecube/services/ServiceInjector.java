package io.scalecube.services;

import io.scalecube.services.Microservices.ProxyContext;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.Router;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.stream.Collectors;

/**
 * 
 * Service Injector scan and injects beans to a given Microservices instance.
 *
 */
public class ServiceInjector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceInjector.class);

  /**
   * Injector builder.
   * 
   * @param Microservices instance to be injected.
   * @return Microservices after injection.
   */
  public static Builder builder(Microservices ms) {
    return new Builder(ms);
  }

  static class Builder {

    private Microservices microservices;

    private Builder(Microservices ms) {
      this.microservices = ms;
    }

    /**
     * inject instances to the microservices instance. either Microservices or ServiceProxy.
     * 
     * @return injected microservices instance.
     */
    public Microservices inject() {
      this.inject(this.microservices);
      return this.microservices;
    }

    /**
     * scan all local service instances and inject a service proxy.
     */
    void inject(Microservices ms) {
      ms.services().stream()
          .filter(instance -> instance.isLocal())
          .collect(Collectors.toList()).forEach(instance -> {
            scanServiceFields(((LocalServiceInstance) instance).serviceObject());
          });
    }

    void scanServiceFields(Object service) {
      for (Field field : service.getClass().getDeclaredFields()) {
        injectField(field, service);
      }
    }

    void injectField(Field field, Object service) {
      if (field.isAnnotationPresent(Inject.class) && field.getType().equals(Microservices.class)) {
        setField(field, service, this.microservices);
      } else if (field.isAnnotationPresent(Inject.class) && isService(field)) {
        setField(field, service, this.microservices.proxy().api(field.getType()).create());
      } else if (field.isAnnotationPresent(ServiceProxy.class) && isService(field)) {
        injectServiceProxy(field, service);
      }
    }

    boolean isService(Field field) {
      return field.getType().isAnnotationPresent(Service.class);
    }

    void injectServiceProxy(Field field, Object service) {
      ServiceProxy annotation = field.getAnnotation(ServiceProxy.class);
      ProxyContext builder = this.microservices.proxy().api(field.getType());
      if (!annotation.router().equals(Router.class)) {
        builder.router(annotation.router());
      }
      if (annotation.timeout() > 0) {
        long nanos = annotation.timeUnit().toNanos(annotation.timeout());
        builder.timeout(Duration.ofNanos(nanos));
      }
      setField(field, service, builder.create());
    }

    void setField(Field field, Object object, Object value) {
      try {
        field.setAccessible(true);
        field.set(object, value);
      } catch (Exception ex) {
        LOGGER.error("failed to set service proxy of type: {} reason:{}", object.getClass().getName(), ex.getMessage());
      }
    }
  }



}
