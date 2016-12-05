package io.scalecube.services.annotations;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServiceDefinition;

import com.google.common.base.Strings;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AnnotationServiceProcessor implements ServiceProcessor {

  @Override
  public Collection<Class<?>> extractServiceInterfaces(Object serviceObject) {
    Class<?>[] interfaces = serviceObject.getClass().getInterfaces();
    return Arrays.stream(interfaces)
        .filter(interfaceClass -> interfaceClass.isAnnotationPresent(Service.class))
        .collect(Collectors.toList());
  }

  @Override
  public ServiceDefinition introspectServiceInterface(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    checkArgument(serviceAnnotation != null, "Not a service interface: %s", serviceInterface);
    String serviceName = resolveServiceName(serviceInterface, serviceAnnotation);

    // Method name
    Map<String, Method> methods = Arrays.stream(serviceInterface.getMethods())
        .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
        .collect(Collectors.toMap(method -> {
          ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
          return resolveMethodName(method, methodAnnotation);
        }, Function.identity()));

    return new ServiceDefinition(serviceInterface, serviceName, Collections.unmodifiableMap(methods));
  }

  private String resolveServiceName(Class<?> serviceInterface, Service serviceAnnotation) {
    return Strings.isNullOrEmpty(serviceAnnotation.value()) ? serviceInterface.getName() : serviceAnnotation.value();
  }

  private String resolveMethodName(Method method, ServiceMethod methodAnnotation) {
    return Strings.isNullOrEmpty(methodAnnotation.value()) ? method.getName() : methodAnnotation.value();
  }

  @Override
  public Set<ServiceDefinition> serviceDefinitions(Object service) {
    return this.extractServiceInterfaces(service).stream()
        .map(foreach -> introspectServiceInterface(foreach))
        .collect(Collectors.toSet());
  }
}
