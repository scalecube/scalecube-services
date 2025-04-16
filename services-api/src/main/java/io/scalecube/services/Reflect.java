package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import io.scalecube.services.annotations.ExecuteOn;
import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.AllowedRole;
import io.scalecube.services.auth.AllowedRoles;
import io.scalecube.services.auth.Secured;
import io.scalecube.services.methods.ServiceRoleDefinition;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class Reflect {

  private Reflect() {
    // Do not instantiate
  }

  /**
   * Extracts parameterized return value of a method.
   *
   * @param method method to inspect
   * @return the generic type of the return value or object
   */
  public static Type parameterizedReturnType(Method method) {
    if (method.isAnnotationPresent(ResponseType.class)) {
      return method.getAnnotation(ResponseType.class).value();
    }

    Type type = method.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      Type actualReturnType = ((ParameterizedType) type).getActualTypeArguments()[0];

      if (ServiceMessage.class.equals(actualReturnType)) {
        return Object.class;
      }

      return actualReturnType;
    } else {
      return Object.class;
    }
  }

  /**
   * Checks if return type of method is {@link ServiceMessage}.
   *
   * @param method method to inspect
   * @return true if return type of method is {@link ServiceMessage}, otherwise false
   */
  public static boolean isReturnTypeServiceMessage(Method method) {
    Type type = method.getGenericReturnType();

    if (type instanceof ParameterizedType) {
      Type actualReturnType = ((ParameterizedType) type).getActualTypeArguments()[0];

      return ServiceMessage.class.equals(actualReturnType);
    }

    return false;
  }

  /**
   * Returns the the type of method parameter. In case the method is annotated with {@link
   * RequestType} this type will always be chosen. If the parameter is generic, eg. {@link String},
   * then actual type will be used. In case there is no annotation and the type is not generic then
   * return the actual type. In case method accepts service message and no RequestType annotation is
   * present then return Object.class
   *
   * @param method in inspection
   * @return type of method parameter
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
   * Checks if the first parameter of method is {@link ServiceMessage}.
   *
   * @param method method to inspect
   * @return true if the first parameter of method is {@link ServiceMessage}, otherwise false
   */
  public static boolean isRequestTypeServiceMessage(Method method) {
    Type[] parameterTypes = method.getGenericParameterTypes();

    if (parameterTypes.length < 1) {
      return false;
    }

    if (parameterTypes[0] instanceof ParameterizedType) {
      //noinspection PatternVariableCanBeUsed
      ParameterizedType parameterizedType = (ParameterizedType) parameterTypes[0];
      return ServiceMessage.class.equals(parameterizedType.getActualTypeArguments()[0]);
    }

    return ServiceMessage.class.equals(parameterTypes[0]);
  }

  /**
   * Returns the parameterized type of a given object.
   *
   * @param object to inspect
   * @return the parameterized type
   */
  @SuppressWarnings("unused")
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
   * Returns the parameterized of the request type of a given object.
   *
   * @return the parameterized type
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
   * Extracts service name from service interface.
   *
   * @param serviceInterface serviceInterface with {@link Service} annotation
   * @return service name
   */
  public static String serviceName(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    if (serviceAnnotation == null) {
      throw new IllegalArgumentException("Not a service interface: " + serviceInterface);
    }
    return serviceAnnotation.value().length() > 0
        ? serviceAnnotation.value()
        : serviceInterface.getName();
  }

  /**
   * Extracts service tags from service interface.
   *
   * @param serviceInterface serviceInterface with {@link Service} annotation
   * @return service tags
   */
  public static Map<String, String> serviceTags(Class<?> serviceInterface) {
    return Reflect.transformArrayToMap(serviceInterface.getAnnotationsByType(Tag.class));
  }

  /**
   * Extracts service tags from service method.
   *
   * @param serviceMethod serviceMethod with {@link ServiceMethod} annotation
   * @return service tags
   */
  public static Map<String, String> serviceMethodTags(Method serviceMethod) {
    return Reflect.transformArrayToMap(serviceMethod.getAnnotationsByType(Tag.class));
  }

  private static Map<String, String> transformArrayToMap(Tag[] array) {
    return array == null || array.length == 0
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(
            Arrays.stream(array).collect(Collectors.toMap(Tag::key, Tag::value)));
  }

  /**
   * Gets service method map from service api.
   *
   * @param serviceInterface with {@link Service} annotation
   * @return service name
   */
  public static Map<String, Method> serviceMethods(Class<?> serviceInterface) {
    return Arrays.stream(serviceInterface.getMethods())
        .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
        .collect(Collectors.toMap(Reflect::methodName, Function.identity()));
  }

  /**
   * Gets service interfaces collections from service instance.
   *
   * @param serviceObject serviceObject that implements service interface with {@link Service}
   *     annotation
   * @return service interface class
   */
  public static Stream<Class<?>> serviceInterfaces(Object serviceObject) {
    Class<?> current = serviceObject.getClass();
    Set<Class<?>> interfaces = new HashSet<>();
    while (current != Object.class) {
      interfaces.addAll(Arrays.asList(current.getInterfaces()));
      current = current.getSuperclass();
    }
    return interfaces.stream()
        .filter(interfaceClass -> interfaceClass.isAnnotationPresent(Service.class));
  }

  /**
   * Extracting method name, checks for annotation {@link ServiceMethod}.
   *
   * @param method method
   * @return method name
   */
  public static String methodName(Method method) {
    ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
    return methodAnnotation.value().length() > 0 ? methodAnnotation.value() : method.getName();
  }

  /**
   * Extracting REST method name, checks for annotation {@link RestMethod}.
   *
   * @param method method
   * @return method name
   */
  public static String restMethod(Method method) {
    RestMethod methodAnnotation = method.getAnnotation(RestMethod.class);
    return methodAnnotation != null ? methodAnnotation.value() : null;
  }

  /**
   * Util function to perform basic validation of service message request.
   *
   * @param method service method
   */
  public static void validateMethodOrThrow(Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType.equals(Void.TYPE)) {
      return;
    } else if (!Publisher.class.isAssignableFrom(returnType)) {
      throw new UnsupportedOperationException("Service method return type can be Publisher only");
    }

    validateResponseType(method);
    validateRequestType(method);

    if (method.getParameterCount() > 1) {
      throw new UnsupportedOperationException(
          "Service method can accept at maximum single parameter");
    }
  }

  private static void validateResponseType(Method method) {
    if (isReturnTypeServiceMessage(method)) {
      if (!method.isAnnotationPresent(ResponseType.class)) {
        throw new UnsupportedOperationException(
            "Return type ServiceMessage cannot be used without @ResponseType method annotation");
      } else if (ServiceMessage.class.equals(method.getAnnotation(ResponseType.class).value())) {
        throw new UnsupportedOperationException(
            "ServiceMessage is not allowed value for @ResponseType");
      }
    }
  }

  private static void validateRequestType(Method method) {
    if (isRequestTypeServiceMessage(method)) {
      if (!method.isAnnotationPresent(RequestType.class)) {
        throw new UnsupportedOperationException(
            "Request type ServiceMessage cannot be used without @RequestType method annotation");
      } else if (ServiceMessage.class.equals(method.getAnnotation(RequestType.class).value())) {
        throw new UnsupportedOperationException(
            "ServiceMessage is not allowed value for @RequestType");
      }
    }
  }

  /**
   * This method is used to get actual {@link CommunicationMode} of service method.
   *
   * <p>The following modes are supported:
   *
   * <ul>
   *   <li>{@link CommunicationMode#REQUEST_CHANNEL} - service has at least one parameter, and the
   *       first parameter is either of {@link Flux} or {@link Publisher}.
   *   <li>{@link CommunicationMode#REQUEST_STREAM} - service return type is {@link Flux}, and
   *       parameter is not {@link Flux}.
   *   <li>{@link CommunicationMode#REQUEST_RESPONSE} - service return type is either {@code
   *       Mono<Pojo>} or {@code Mono<Void>}.
   * </ul>
   *
   * @param method service method
   * @return {@link CommunicationMode} of service method, or throws {@link IllegalArgumentException}
   */
  public static CommunicationMode communicationMode(Method method) {
    Class<?> returnType = method.getReturnType();
    if (isRequestChannel(method)) {
      return REQUEST_CHANNEL;
    } else if (returnType.isAssignableFrom(Flux.class)) {
      return REQUEST_STREAM;
    } else if (returnType.isAssignableFrom(Mono.class) || returnType.isAssignableFrom(Void.TYPE)) {
      return REQUEST_RESPONSE;
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

  /**
   * Checks whether given class have annotation {@link Service}.
   *
   * @param type type to check for {@link Service} annotation
   * @return result
   */
  public static boolean isService(Class<?> type) {
    return type.isAnnotationPresent(Service.class);
  }

  /**
   * Retrives {@link Secured} annotation. Annotation is expected to be declared on service method,
   * or on the service class. If declared on both - service method declaration takes precedence.
   *
   * @param method service method
   * @return {@link Secured} annotation, or null
   */
  public static Secured secured(Method method) {
    Secured secured = method.getAnnotation(Secured.class);

    // If @Secured annotation is not present on service method, then find it on service class

    if (secured == null) {
      for (var clazz = method.getDeclaringClass(); clazz != null; clazz = clazz.getSuperclass()) {
        if (clazz.isAnnotationPresent(Secured.class)) {
          secured = clazz.getAnnotation(Secured.class);
          break;
        }
      }
    }

    return secured;
  }

  /**
   * Parsing collection of service roles from service method.
   *
   * @param method service method
   * @return {@link ServiceRoleDefinition} objects
   */
  public static Collection<ServiceRoleDefinition> serviceRoles(Method method) {
    AllowedRole[] allowedRoles = null;

    if (method.isAnnotationPresent(AllowedRoles.class)) {
      allowedRoles = method.getAnnotation(AllowedRoles.class).value();
    } else if (method.isAnnotationPresent(AllowedRole.class)) {
      allowedRoles = method.getAnnotationsByType(AllowedRole.class);
    }

    // If @AllowedRoles/@AllowedRole annotations are not present on service method, then find them
    // on service class

    if (allowedRoles == null) {
      for (var clazz = method.getDeclaringClass(); clazz != null; clazz = clazz.getSuperclass()) {
        if (clazz.isAnnotationPresent(AllowedRoles.class)) {
          allowedRoles = clazz.getAnnotation(AllowedRoles.class).value();
          break;
        }
        if (clazz.isAnnotationPresent(AllowedRole.class)) {
          allowedRoles = clazz.getAnnotationsByType(AllowedRole.class);
          break;
        }
      }
    }

    return allowedRoles != null
        ? Arrays.stream(allowedRoles)
            .map(
                allowedRole ->
                    new ServiceRoleDefinition(
                        allowedRole.name(),
                        new HashSet<>(Arrays.asList(allowedRole.permissions()))))
            .collect(Collectors.toSet())
        : Set.of();
  }

  /**
   * Parsing collection of service role names from service method.
   *
   * @param method service method
   * @return service role names
   */
  public static Collection<String> allowedRoles(Method method) {
    return serviceRoles(method).stream()
        .map(ServiceRoleDefinition::role)
        .collect(Collectors.toSet());
  }

  /**
   * Parsing annotation {@code ExecuteOn} and extracts {@link Scheduler} instance as result.
   *
   * @param method service method
   * @param schedulers schedulers map
   * @return {@link Scheduler} instance
   */
  public static Scheduler executeOnScheduler(Method method, Map<String, Scheduler> schedulers) {
    if (schedulers == null) {
      return Schedulers.immediate();
    }

    Class<?> clazz = method.getDeclaringClass();

    if (method.isAnnotationPresent(ExecuteOn.class)) {
      final var executeOn = method.getAnnotation(ExecuteOn.class);
      final var name = executeOn.value();
      final var scheduler = schedulers.get(name);
      if (scheduler == null) {
        throw new IllegalArgumentException(
            "Wrong @ExecuteOn definition on "
                + clazz.getName()
                + "."
                + method.getName()
                + ": scheduler(name="
                + name
                + ") cannot be found");
      }
      return scheduler;
    }

    // If @ExecuteOn annotation is not present on service method, then find it on service class

    ExecuteOn executeOn = null;
    for (; clazz != null; clazz = clazz.getSuperclass()) {
      executeOn = clazz.getAnnotation(ExecuteOn.class);
      if (executeOn != null) {
        break;
      }
    }

    if (executeOn == null) {
      return Schedulers.immediate();
    }

    final var name = executeOn.value();
    final var scheduler = schedulers.get(name);
    if (scheduler == null) {
      throw new IllegalArgumentException(
          "Wrong @ExecuteOn definition on "
              + clazz.getName()
              + "."
              + method.getName()
              + ": scheduler(name="
              + name
              + ") cannot be found");
    }

    return scheduler;
  }
}
