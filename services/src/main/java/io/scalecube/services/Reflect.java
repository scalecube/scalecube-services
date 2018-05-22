package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;
import static java.util.Objects.requireNonNull;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Null;
import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;

import com.google.common.base.Strings;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service Injector scan and injects beans to a given Microservices instance.
 *
 */
public class Reflect {


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
    private static final Logger LOGGER = LoggerFactory.getLogger(Reflect.class);

    private Microservices microservices;

    Builder(Microservices ms) {
      this.microservices = ms;
    }

    /**
     * inject instances to the microservices instance. either Microservices or ServiceProxy.
     *
     * @return injected microservices instance.
     */
    public Microservices inject() {
      this.inject(this.microservices.services());
      return this.microservices;
    }

    /**
     * scan all local service instances and inject a service proxy.
     */
    private void inject(Collection<Object> collection) {
      for (Object instance : collection) {
        scanServiceFields(instance);
        processPostConstruct(instance);
      }
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
                } else if (isService(mapper.getType())) {
                  return this.microservices.call().create().api(mapper.getType());
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
        Inject injection = field.getAnnotation(Inject.class);
        Class<? extends Router> routerClass = injection.router();
        if (routerClass.isAnnotationPresent(Null.class)) {
          routerClass = RoundRobinServiceRouter.class;
        }
        Router router = Optional.of(routerClass).map(Routers::getRouter).orElseGet(() -> {
          LOGGER.warn("Unable to inject router {}, using RoundRobin", injection.router());
          return Routers.getRouter(RoundRobinServiceRouter.class);
        });
        setField(field, service, this.microservices.call().router(router).create().api(field.getType()));
      }
    }

    private static boolean isService(Class<?> type) {
      return type.isAnnotationPresent(Service.class);
    }

    private static void setField(Field field, Object object, Object value) {
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
   * Util function returns the the Type of method parameter [0] or Void.Type in case 0 parameters. in case the method is
   * annotated with @RequestType this type will always be chosen. if the parameter is generic eg. <String> the actual
   * type will be used. in case there is no annotation and the type is not generic then return the actual type. in case
   * method accepts service message and no RequestType annotation is present then return Object.class
   *
   * @param method in inspection.
   * @return type of parameter [0] or void
   */
  public static Class<?> requestType(Method method) {
    if (method.getParameterTypes().length > 0) {
      if (method.isAnnotationPresent(RequestType.class)) {
        return method.getAnnotation(RequestType.class).value();
      } else {
        if (method.getGenericParameterTypes()[0] instanceof ParameterizedType) {
          try {
            return Class.forName(parameterizedRequestType(method).getTypeName());
          } catch (ClassNotFoundException e) {
            return Object.class;
          }
        } else if (ServiceMessage.class.equals(method.getParameterTypes()[0])) {
          return Object.class;
        } else {
          return method.getParameterTypes()[0];
        }

      }
    } else {
      return Void.TYPE;
    }
  }

  /**
   * Util function to determine if method has parameter of type {@link ServiceMessage}.
   *
   * @param method in inspection.
   * @return true if request paramter is of type {@link ServiceMessage} and false otherwise.
   */
  public static boolean isRequestTypeServiceMessage(Method method) {
    return Reflect.requestType(method).isAssignableFrom(ServiceMessage.class);
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
   * Util function that returns the parameterized of the request Type of a given object.
   * 
   * @return the parameterized Type of a given object or Object class if unknown.
   */
  public static Type parameterizedRequestType(Method method) {
    if (method != null && method.getGenericParameterTypes().length > 0) {
      Type type = method.getGenericParameterTypes()[0];
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
    requireNonNull(serviceAnnotation != null, String.format("Not a service interface: %s", serviceInterface));
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
   * Invoke service method by a given service message publisher.
   *
   * @param serviceObject instance to invoke its method.
   * @param method method to invoke.
   * @param publisher stream message request containing data or message to invoke.
   * @param mode communication mode to determine what java method sygnature to invoke.
   * @return invoke result; returns publisher.
   */
  public static Publisher<?> invokePublisher(Object serviceObject,
      Method method,
      CommunicationMode mode,
      Publisher<?> publisher) {
    switch (mode) {
      case FIRE_AND_FORGET:
      case REQUEST_RESPONSE:
      case REQUEST_STREAM:
        return Flux.from(publisher).flatMap(request -> invokePublisher(serviceObject, method, request));
      case REQUEST_CHANNEL:
        return Flux.from(publisher).transform(publisher1 -> invokePublisher(serviceObject, method, publisher1));
      default:
        throw new IllegalArgumentException("Communication mode is not supported: " + method);
    }
  }

  public static String methodName(Method method) {
    ServiceMethod annotation = method.getAnnotation(ServiceMethod.class);
    return Strings.isNullOrEmpty(annotation.value()) ? method.getName() : annotation.value();
  }

  public static String qualifier(Class<?> serviceInterface, Method method) {
    return Qualifier.asString(serviceName(serviceInterface), methodName(method));
  }

  /**
   * Util function to perform basic validation of service message request.
   *
   * @param method service method.
   */
  public static void validateMethodOrThrow(Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType.equals(Void.TYPE)) {
      return;
    } else if (!Publisher.class.isAssignableFrom(returnType)) {
      throw new UnsupportedOperationException("Service method return type can be Publisher only");
    }
    if (method.getParameters().length > 1) {
      throw new UnsupportedOperationException("Service method can accept 0 or 1 parameters only");
    }
  }

  public static CommunicationMode communicationMode(Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType.isAssignableFrom(Void.TYPE)) {
      return FIRE_AND_FORGET;
    } else if (returnType.isAssignableFrom(Mono.class)) {
      return REQUEST_RESPONSE;
    } else if (returnType.isAssignableFrom(Flux.class)) {
      Class<?>[] reqTypes = method.getParameterTypes();
      boolean hasFluxAsReqParam = reqTypes.length > 0
          && Flux.class.isAssignableFrom(reqTypes[0]);
      return hasFluxAsReqParam ? REQUEST_CHANNEL : REQUEST_STREAM;
    } else {
      throw new IllegalArgumentException(
          "Service method is not supported (check return type or parameter type): " + method);
    }
  }

  private static Publisher<?> invokePublisher(Object serviceObject, Method method, Object request) {
    try {
      if (method.getParameters().length == 0) {
        return Flux.from((Publisher<?>) method.invoke(serviceObject));
      } else {
        return Flux.from((Publisher<?>) method.invoke(serviceObject, request));
      }
    } catch (InvocationTargetException ex) {
      return Flux.error(Optional.ofNullable(ex.getCause()).orElse(ex));
    } catch (Throwable ex) {
      return Flux.error(ex);
    }
  }

  private static Publisher<?> invokePublisher(Object serviceObject, Method method, Publisher<?> publisher) {
    try {
      if (method.getParameters().length == 0) {
        return Flux.from((Publisher<?>) method.invoke(serviceObject));
      } else {
        return Flux.from((Publisher<?>) method.invoke(serviceObject, publisher));
      }
    } catch (InvocationTargetException ex) {
      return Flux.error(Optional.ofNullable(ex.getCause()).orElse(ex));
    } catch (Throwable ex) {
      return Flux.error(ex);
    }
  }
}
