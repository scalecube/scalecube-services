package io.scalecube.services;

import static io.scalecube.utils.Preconditions.checkArgument;

import io.scalecube.services.Microservices.ProxyContext;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.Router;

import io.scalecube.utils.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service Injector scan and injects beans to a given Microservices instance.
 *
 */
public class Reflect {
  private static final Logger LOGGER = LoggerFactory.getLogger(Reflect.class);

  /**
   * Injector builder.
   * 
   * @param microservices instance to be injected.
   * @return Builder for injection.
   */
  public static Builder builder(Microservices microservices) {
    return new Builder(microservices);
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
    private void inject(Microservices microservices) {
      microservices.services().stream()
          .filter(instance -> instance.isLocal())
          .collect(Collectors.toList()).forEach(instance -> {
            scanServiceFields(((LocalServiceInstance) instance).serviceObject());
          });
    }

    private void scanServiceFields(Object service) {
      for (Field field : service.getClass().getDeclaredFields()) {
        injectField(field, service);
      }
    }

    private void injectField(Field field, Object service) {
      if (field.isAnnotationPresent(Inject.class) && field.getType().equals(Microservices.class)) {
        setField(field, service, this.microservices);
      } else if (field.isAnnotationPresent(Inject.class) && isService(field)) {
        setField(field, service, this.microservices.proxy().api(field.getType()).create());
      } else if (field.isAnnotationPresent(ServiceProxy.class) && isService(field)) {
        injectServiceProxy(field, service);
      }
    }

    private boolean isService(Field field) {
      return field.getType().isAnnotationPresent(Service.class);
    }

    private void injectServiceProxy(Field field, Object service) {
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

    private void setField(Field field, Object object, Object value) {
      try {
        field.setAccessible(true);
        field.set(object, value);
      } catch (Exception ex) {
        LOGGER.error("failed to set service proxy of type: {} reason:{}", object.getClass().getName(), ex.getMessage());
      }
    }
  }

  /**
   * extract parameterized return value of a method.
   * 
   * @param method to extract type from.
   * @return the generic type of the return value or object.
   */
  public static Type parameterizedReturnType(Method method) {
    Type type = method.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getActualTypeArguments()[0];
    } else {
      return Object.class;
    }
  }

  /**
   * Util function that returns the parameterizedType of a given object.
   * 
   * @param object to inspect
   * @return the parameterized Type of a given object or Object class if unknown.
   */
  public static Type parameterizedType(Object object) {
    if (object != null) {
      Type type = object.getClass().getGenericSuperclass();
      if (type instanceof ParameterizedType) {
        return ((ParameterizedType) type).getActualTypeArguments()[0];
      }
    }
    return Object.class;
  }

  /**
   * Util function to extract service name from service api.
   * 
   * @param serviceInterface with @Service annotation.
   * @return service name.
   */
  public static String serviceName(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    checkArgument(serviceAnnotation != null, "Not a service interface: %s", serviceInterface);
    return Strings.isNullOrEmpty(serviceAnnotation.value()) ? serviceInterface.getName() : serviceAnnotation.value();
  }

  /**
   * Util function to get service Method map from service api.
   * 
   * @param serviceInterface with @Service annotation.
   * @return service name.
   */
  public static Map<String, Method> serviceMethods(Class<?> serviceInterface) {
    Map<String, Method> methods = Arrays.stream(serviceInterface.getMethods())
        .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
        .collect(Collectors.toMap(method -> {
          ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
          return Strings.isNullOrEmpty(methodAnnotation.value()) ? method.getName() : methodAnnotation.value();
        }, Function.identity()));

    return Collections.unmodifiableMap(methods);
  }

  /**
   * Util function to get service interfaces collections from service instance.
   * 
   * @param serviceObject with extends service interface with @Service annotation.
   * @return service interface class.
   */
  public static Collection<Class<?>> serviceInterfaces(Object serviceObject) {
    Class<?>[] interfaces = serviceObject.getClass().getInterfaces();
    return Arrays.stream(interfaces)
        .filter(interfaceClass -> interfaceClass.isAnnotationPresent(Service.class))
        .collect(Collectors.toList());
  }

}
