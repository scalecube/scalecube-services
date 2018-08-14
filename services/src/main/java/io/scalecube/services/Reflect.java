package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.routing.Router;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service Injector scan and injects beans to a given Microservices instance.
 *
 */
public final class Reflect {

  private static final Logger LOGGER = LoggerFactory.getLogger(Reflect.class);

  private Reflect() {
    // Do not instantiate
  }


  /**
   * Inject instances to the microservices instance. either Microservices or ServiceProxy. Scan all local service
   * instances and inject a service proxy.
   *
   * @param microservices microservices instance
   * @param services services set
   * @return microservices instance
   */
  public static Microservices inject(Microservices microservices, Collection<Object> services) {
    services.forEach(service -> Arrays.stream(service.getClass().getDeclaredFields())
        .forEach(field -> injectField(microservices, field, service)));
    services.forEach(service -> processAfterConstruct(microservices, service));
    return microservices;
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

  public static Map<Method, MethodInfo> methodsInfo(Class<?> serviceInterface) {
    return Collections.unmodifiableMap(serviceMethods(serviceInterface).values().stream()
        .collect(Collectors.toMap(Function.identity(),
            method1 -> new MethodInfo(
                serviceName(serviceInterface),
                methodName(method1),
                parameterizedReturnType(method1),
                communicationMode(method1),
                method1.getParameterCount(),
                requestType(method1)))));
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
    if (serviceAnnotation == null) {
      throw new IllegalArgumentException(String.format("Not a service interface: %s", serviceInterface));
    }
    return serviceAnnotation.value().length() > 0 ? serviceAnnotation.value() : serviceInterface.getName();
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
        .collect(Collectors.toMap(Reflect::methodName, Function.identity()));

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

  public static String methodName(Method method) {
    ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
    return methodAnnotation.value().length() > 0 ? methodAnnotation.value() : method.getName();
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
    if (method.getParameterCount() > 1) {
      throw new UnsupportedOperationException("Service method can accept 0 or 1 parameters only");
    }
  }

  public static CommunicationMode communicationMode(Method method) {
    Class<?> returnType = method.getReturnType();
    if (isRequestChannel(method)) {
      return REQUEST_CHANNEL;
    } else if (returnType.isAssignableFrom(Flux.class)) {
      return REQUEST_STREAM;
    } else if (returnType.isAssignableFrom(Mono.class)) {
      return REQUEST_RESPONSE;
    } else if (returnType.isAssignableFrom(Void.TYPE)) {
      return FIRE_AND_FORGET;
    } else {
      throw new IllegalArgumentException(
          "Service method is not supported (check return type or parameter type): " + method);
    }
  }

  private static boolean isRequestChannel(Method method) {
    Class<?>[] reqTypes = method.getParameterTypes();
    return reqTypes.length > 0
        && (Flux.class.isAssignableFrom(reqTypes[0])
            || Publisher.class.isAssignableFrom(reqTypes[0]));
  }

  private static void injectField(Microservices microservices, Field field, Object service) {
    if (field.isAnnotationPresent(Inject.class) && field.getType().equals(Microservices.class)) {
      setField(field, service, microservices);
    } else if (field.isAnnotationPresent(Inject.class) && isService(field.getType())) {
      Inject injection = field.getAnnotation(Inject.class);
      Class<? extends Router> routerClass = injection.router();

      final ServiceCall.Call call = microservices.call();

      if (!routerClass.isInterface()) {
        call.router(routerClass);
      }

      final Object targetProxy = call.create().api(field.getType());

      setField(field, service, targetProxy);
    }
  }

  private static void setField(Field field, Object object, Object value) {
    try {
      field.setAccessible(true);
      field.set(object, value);
    } catch (Exception ex) {
      LOGGER.error("failed to set service proxy of type: {} reason:{}", object.getClass().getName(), ex.getMessage());
    }
  }

  private static void processAfterConstruct(Microservices microservices, Object targetInstance) {
    Method[] declaredMethods = targetInstance.getClass().getDeclaredMethods();
    Arrays.stream(declaredMethods)
        .filter(method -> method.isAnnotationPresent(AfterConstruct.class))
        .forEach(afterConstructMethod -> {
          try {
            afterConstructMethod.setAccessible(true);
            Object[] parameters = Arrays.stream(afterConstructMethod.getParameters()).map(mapper -> {
              if (mapper.getType().equals(Microservices.class)) {
                return microservices;
              } else if (isService(mapper.getType())) {
                return microservices.call().create().api(mapper.getType());
              } else {
                return null;
              }
            }).toArray();
            afterConstructMethod.invoke(targetInstance, parameters);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  private static boolean isService(Class<?> type) {
    return type.isAnnotationPresent(Service.class);
  }
}
