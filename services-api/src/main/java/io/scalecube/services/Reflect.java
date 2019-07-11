package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Auth;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.methods.MethodInfo;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Reflect {

  private Reflect() {
    // Do not instantiate
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
   * Util function returns the the Type of method parameter [0] or Void.Type in case 0 parameters.
   * in case the method is annotated with @RequestType this type will always be chosen. if the
   * parameter is generic eg. &lt;String&gt; the actual type will be used. in case there is no
   * annotation and the type is not generic then return the actual type. in case method accepts
   * service message and no RequestType annotation is present then return Object.class
   *
   * @param method in inspection.
   * @return type of parameter [0] or void
   */
  public static Class<?> requestType(Method method) {
    if (method.getParameterTypes().length > 0) {
      if (method.isAnnotationPresent(RequestType.class)) {
        return method.getAnnotation(RequestType.class).value();
      } else {
        if (method.getParameters()[0].isAnnotationPresent(Principal.class)) {
          return Void.TYPE;
        }

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

  /**
   * Parse <code>serviceInterface</code> class and puts available methods annotated by {@link
   * ServiceMethod} annotation to {@link Method} -> {@link MethodInfo} mapping.
   *
   * @param serviceInterface - service interface to be parsed.
   * @return - mapping form available service methods of the <code>serviceInterface</code> to their
   *     descriptions
   */
  public static Map<Method, MethodInfo> methodsInfo(Class<?> serviceInterface) {
    return Collections.unmodifiableMap(
        serviceMethods(serviceInterface).values().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    method1 ->
                        new MethodInfo(
                            serviceName(serviceInterface),
                            methodName(method1),
                            parameterizedReturnType(method1),
                            communicationMode(method1),
                            method1.getParameterCount(),
                            requestType(method1),
                            isAuth(method1)))));
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
      throw new IllegalArgumentException(
          String.format("Not a service interface: %s", serviceInterface));
    }
    return serviceAnnotation.value().length() > 0
        ? serviceAnnotation.value()
        : serviceInterface.getName();
  }

  /**
   * Util function to extract service tags from service api.
   *
   * @param serviceInterface with @Service annotation.
   * @return service tags
   */
  public static Map<String, String> serviceTags(Class<?> serviceInterface) {
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    if (serviceAnnotation == null) {
      throw new IllegalArgumentException(
          String.format("Not a service interface: %s", serviceInterface));
    }
    String[] rawTags = serviceAnnotation.tags();
    if (rawTags.length % 2 == 1) {
      throw new IllegalStateException(String.format("Invalid tags for '%s'", serviceInterface));
    }
    return transformArrayToMap(rawTags);
  }

  /**
   * Util function to extract service tags from service method api.
   *
   * @param serviceMethod with @ServiceMethod annotation.
   * @return service tags
   */
  public static Map<String, String> serviceMethodTags(Method serviceMethod) {
    ServiceMethod serviceMethodAnnotation = serviceMethod.getAnnotation(ServiceMethod.class);
    if (serviceMethodAnnotation == null) {
      throw new IllegalArgumentException(
          String.format("Not a service interface: %s", serviceMethodAnnotation));
    }
    String[] rawTags = serviceMethodAnnotation.tags();
    if (rawTags.length % 2 == 1) {
      throw new IllegalStateException(
          String.format("Invalid tags for service method '%s'", serviceMethod.getName()));
    }
    return Reflect.transformArrayToMap(rawTags);
  }

  private static Map<String, String> transformArrayToMap(String[] array) {
    if (array.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, String> tags = new HashMap<>();
    for (int keyIndex = 0; keyIndex < array.length; keyIndex += 2) {
      final int valueIndex = keyIndex + 1;
      String tagName = array[keyIndex];
      String tagValue = array[valueIndex];
      tags.merge(
          tagName,
          tagValue,
          (o, n) -> {
            throw new IllegalStateException(String.format("Duplicate tag %s", tagName));
          });
    }
    return Collections.unmodifiableMap(tags);
  }

  /**
   * Util function to get service Method map from service api.
   *
   * @param serviceInterface with @Service annotation.
   * @return service name.
   */
  public static Map<String, Method> serviceMethods(Class<?> serviceInterface) {
    Map<String, Method> methods =
        Arrays.stream(serviceInterface.getMethods())
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

  /**
   * Handy method to get qualifier String from service's interface and method.
   *
   * @param serviceInterface service interface to get qualifier for
   * @param method service's method to get qualifier for
   * @return
   */
  public static String qualifier(Class<?> serviceInterface, Method method) {
    return Qualifier.asString(Reflect.serviceName(serviceInterface), Reflect.methodName(method));
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

    validatePrincipalParameter(method);

    if (method.getParameterCount() > 2) {
      throw new UnsupportedOperationException("Service method can accept maximum 2 parameters");
    }
  }

  private static void validatePrincipalParameter(Method method) {
    Parameter[] parameters = method.getParameters();

    if (!isAuth(method)) {
      for (Parameter parameter : parameters) {
        if (parameter.isAnnotationPresent(Principal.class)) {
          throw new UnsupportedOperationException(
              "@Principal can be used only for parameter of @Auth method");
        }
      }
    }

    if (method.getParameterCount() == 2) {
      if (parameters[0].isAnnotationPresent(Principal.class)) {
        throw new UnsupportedOperationException(
            "@Principal cannot be the first parameter if parameters count equals 2");
      }

      if (!parameters[1].isAnnotationPresent(Principal.class)) {
        throw new UnsupportedOperationException(
            "The second parameter can be only @Principal (optional)");
      }
    }
  }

  /**
   * This method is used to get catual {@link CommunicationMode} os service method.
   *
   * <p>The following modes are supported:
   *
   * <ul>
   *   <li>{@link CommunicationMode#REQUEST_CHANNEL} - service has at least one parameter,and the
   *       first parameter is either of type return type {@link Flux} or {@link Publisher};
   *   <li>{@link CommunicationMode#REQUEST_STREAM} - service's return type is {@link Flux}, and
   *       parameter is not {@link Flux};
   *   <li>{@link CommunicationMode#REQUEST_RESPONSE} - service's return type is Mono;
   *   <li>{@link CommunicationMode#FIRE_AND_FORGET} - service returns void;
   * </ul>
   *
   * @param method - Service method to be analyzed.
   * @return - {@link CommunicationMode} of service method. If method does not correspond to any of
   *     supported modes, throws {@link IllegalArgumentException}
   */
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

  public static void setField(Field field, Object object, Object value)
      throws IllegalAccessException {
    field.setAccessible(true);
    field.set(object, value);
  }

  public static boolean isService(Class<?> type) {
    return type.isAnnotationPresent(Service.class);
  }

  public static boolean isAuth(Method method) {
    return method.isAnnotationPresent(Auth.class)
        || method.getDeclaringClass().isAnnotationPresent(Auth.class);
  }
}
