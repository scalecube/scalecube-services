package io.scalecube.gateway.clientsdk;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;
import static java.util.Objects.requireNonNull;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.MethodInfo;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class Reflect {

  static Map<Method, MethodInfo> methodsInfo(Class<?> serviceInterface) {
    return Collections.unmodifiableMap(
        serviceMethods(serviceInterface)
            .values()
            .stream()
            .collect(
                Collectors.toMap(
                    method -> method,
                    method1 ->
                        new MethodInfo(
                            serviceName(serviceInterface),
                            methodName(method1),
                            parameterizedReturnType(method1),
                            communicationMode(method1),
                            method1.getParameterCount(),
                            requestType(method1)))));
  }

  /**
   * Util function returns the the Type of method parameter [0] or Void.Type in case 0 parameters.
   * in case the method is annotated with @RequestType this type will always be chosen. if the
   * parameter is generic eg. <String> the actual type will be used. in case there is no annotation
   * and the type is not generic then return the actual type. in case method accepts service message
   * and no RequestType annotation is present then return Object.class
   *
   * @param method in inspection.
   * @return type of parameter [0] or void
   */
  private static Class<?> requestType(Method method) {
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
   * extract parameterized return value of a method.
   *
   * @param method to extract type from.
   * @return the generic type of the return value or object.
   */
  private static Class<?> parameterizedReturnType(Method method) {
    Type type = method.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      try {
        return Class.forName(
            (((ParameterizedType) type).getActualTypeArguments()[0]).getTypeName());
      } catch (ClassNotFoundException e) {
        return Object.class;
      }
    } else {
      return Object.class;
    }
  }

  /**
   * Util function that returns the parameterized of the request Type of a given object.
   *
   * @return the parameterized Type of a given object or Object class if unknown.
   */
  private static Type parameterizedRequestType(Method method) {
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
  private static String serviceName(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    requireNonNull(
        serviceAnnotation != null, String.format("Not a service interface: %s", serviceInterface));
    return isNullOrEmpty(serviceAnnotation.value())
        ? serviceInterface.getName()
        : serviceAnnotation.value();
  }

  /**
   * Util function to get service Method map from service api.
   *
   * @param serviceInterface with @Service annotation.
   * @return service name.
   */
  private static Map<String, Method> serviceMethods(Class<?> serviceInterface) {
    Map<String, Method> methods =
        Arrays.stream(serviceInterface.getMethods())
            .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
            .collect(
                Collectors.toMap(
                    method -> {
                      ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
                      return isNullOrEmpty(methodAnnotation.value())
                          ? method.getName()
                          : methodAnnotation.value();
                    },
                    Function.identity()));

    return Collections.unmodifiableMap(methods);
  }

  private static String methodName(Method method) {
    ServiceMethod annotation = method.getAnnotation(ServiceMethod.class);
    return isNullOrEmpty(annotation.value()) ? method.getName() : annotation.value();
  }

  private static CommunicationMode communicationMode(Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType.isAssignableFrom(Void.TYPE)) {
      return FIRE_AND_FORGET;
    } else if (returnType.isAssignableFrom(Mono.class)) {
      return REQUEST_RESPONSE;
    } else if (returnType.isAssignableFrom(Flux.class)) {
      Class<?>[] reqTypes = method.getParameterTypes();
      boolean hasFluxAsReqParam =
          reqTypes.length > 0
              && (Flux.class.isAssignableFrom(reqTypes[0])
                  || Publisher.class.isAssignableFrom(reqTypes[0]));

      return hasFluxAsReqParam ? REQUEST_CHANNEL : REQUEST_STREAM;
    } else {
      throw new IllegalArgumentException(
          "Service method is not supported (check return type or parameter type): " + method);
    }
  }

  private static boolean isNullOrEmpty(String string) {
    return string == null || string.length() == 0; // string.isEmpty() in Java 6
  }
}
