package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;

import com.google.common.base.Strings;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
  public static Class<?> requestType(Method method) {
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


  public static <T> Publisher<T> invokeMessage(Object serviceObject, Method method, final ServiceMessage request)
      throws Exception {
    Object result = invoke(serviceObject, method, request);
    Class<?> returnType = method.getReturnType();
    if (returnType.isAssignableFrom(Publisher.class)) {
      return (Publisher<T>) result;
    } else {
      // should we later support 2 parameters? message and the Stream processor?
      throw new UnsupportedOperationException("Service Method can return of type Publisher only");
    }
  }

  /**
   * invoke a java method by a given ServiceMessage.
   *
   * @param serviceObject instance to invoke its method.
   * @param method method to invoke.
   * @param request stream message request containing data or message to invoke.
   * @return invoke result.
   * @throws Exception in case method expects more then one parameter
   */
  @SuppressWarnings("unchecked")
  public static <T> T invoke(Object serviceObject, Method method, final ServiceMessage request) throws Exception {
    // handle invoke
    if (method.getParameters().length == 0) { // method expect no params.
      return (T) method.invoke(serviceObject);
    } else if (method.getParameters().length == 1) { // method expect 1 param.
      if (method.getParameters()[0].getType().isAssignableFrom(ServiceMessage.class)) {
        return (T) method.invoke(serviceObject, request);
      } else {
        return (T) method.invoke(serviceObject, new Object[] {request.data()});
      }
    } else {
      // should we later support 2 parameters? message and the Stream processor?
      throw new UnsupportedOperationException("Service Method can accept 0 or 1 paramters only!");
    }
  }
}
