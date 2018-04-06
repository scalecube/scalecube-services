package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.Router;
import io.scalecube.streams.StreamMessage;

import com.google.common.base.Strings;

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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

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
            this.processPostConstruct(((LocalServiceInstance) instance).serviceObject());
          });
    }

    private void processPostConstruct(Object targetInstance) {
      Method[] declaredMethods = targetInstance.getClass().getDeclaredMethods();
      Arrays.stream(declaredMethods)
          .filter(method -> method.isAnnotationPresent(PostConstruct.class))
          .forEach(postConstructMethod -> {
            try {
              postConstructMethod.setAccessible(true);
              Object[] paramters = Arrays.asList(postConstructMethod.getParameters()).stream().map(mapper -> {
                if (mapper.getType().equals(Microservices.class)) {
                  return this.microservices;
                } else if (mapper.isAnnotationPresent(ServiceProxy.class)) {
                  return newServiceCall(mapper.getAnnotation(ServiceProxy.class));
                } else if (isService(mapper.getType())) {
                  return this.microservices.call().api(mapper.getType());
                } else {
                  return null;
                }
              }).collect(Collectors.toList()).toArray();
              postConstructMethod.invoke(targetInstance, paramters);
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
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
      } else if (field.isAnnotationPresent(Inject.class) && isService(field.getType())) {
        setField(field, service, this.microservices.call().api(field.getType()));
      } else if (field.isAnnotationPresent(ServiceProxy.class) && isService(field.getType())) {
        injectServiceProxy(field, service);
      }
    }

    private boolean isService(Class type) {
      return type.isAnnotationPresent(Service.class);
    }

    private void injectServiceProxy(Field field, Object service) {
      ServiceProxy annotation = field.getAnnotation(ServiceProxy.class);
      Call builder = newServiceCall(annotation);
      setField(field, service, builder);
    }

    private Call newServiceCall(ServiceProxy annotation) {
      Call builder = this.microservices.call();
      if (!annotation.router().equals(Router.class)) {
        builder.router(this.microservices.router(annotation.router()));
      }
      if (annotation.timeout() > 0) {
        long nanos = annotation.timeUnit().toNanos(annotation.timeout());
        builder.timeout(Duration.ofNanos(nanos));
      }
      return builder;
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
  public static Class<?> parameterizedReturnType(Method method) {
    Type type = method.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      try {
        return Class.forName((((ParameterizedType) type).getActualTypeArguments()[0]).getTypeName());
      } catch (ClassNotFoundException e) {
        return Object.class;
      }
    } else {
      return Object.class;
    }
  }

  /**
   * Util function returns the the Type of method parameter [0] or Void.Type in case 0 parameters.
   *
   * @param method in inspection.
   * @return type of parameter [0] or void
   */
  public static Type requestType(Method method) {
    if (method.getParameterTypes().length > 0) {
      return method.getParameterTypes()[0];
    }
    return Void.TYPE;
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

  /**
   * invoke a java method by a given StreamMessage.
   *
   * @param serviceObject instance to invoke its method.
   * @param method method to invoke.
   * @param request stream message request containing data or message to invoke.
   * @return invoke result.
   * @throws Exception in case method expects more then one parameter
   */
  @SuppressWarnings("unchecked")
  public static <T> T invoke(Object serviceObject, Method method, final StreamMessage request) throws Exception {
    // handle invoke
    if (method.getParameters().length == 0) { // method expect no params.
      return (T) method.invoke(serviceObject);
    } else if (method.getParameters().length == 1) { // method expect 1 param.
      if (method.getParameters()[0].getType().isAssignableFrom(StreamMessage.class)) {
        return (T) method.invoke(serviceObject, request);
      } else {
        T invoke = (T) method.invoke(serviceObject, new Object[]{request.data()});
        return invoke;
      }
    } else {
      // should we later support 2 parameters? message and the Stream processor?
      throw new UnsupportedOperationException("Service Method can accept 0 or 1 paramters only!");
    }
  }


}
