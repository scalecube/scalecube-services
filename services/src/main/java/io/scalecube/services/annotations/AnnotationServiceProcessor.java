package io.scalecube.services.annotations;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServiceDefinition;

import com.google.common.base.Strings;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnnotationServiceProcessor implements ServiceProcessor {

  @Override
  public Collection<Class<?>> extractServiceInterfaces(Object serviceObject) {
    Class<?>[] interfaces = serviceObject.getClass().getInterfaces();
    List<Class<?>> serviceInterfaces = new ArrayList<>();
    for (Class<?> interfaceClass : interfaces) {
      if (interfaceClass.isAnnotationPresent(Service.class)) {
        serviceInterfaces.add(interfaceClass);
      }
    }
    return serviceInterfaces;
  }

  @Override
  public ServiceDefinition introspectServiceInterface(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    checkArgument(serviceAnnotation != null, "Not a service interface: %s", serviceInterface);
    String serviceName = resolveServiceName(serviceInterface, serviceAnnotation);

    // Method name
    Map<String, Method> methods = new HashMap<>();
    for (Method method : serviceInterface.getMethods()) {
      if (method.isAnnotationPresent(ServiceMethod.class)) {
        ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
        String methodName = resolveMethodName(method, methodAnnotation);
        if (methods.containsKey(methodName)) {
          throw new IllegalStateException("Service method with name '" + methodName + "' already exists");
        }
        methods.put(methodName, method);
      }
    }
    ServiceDefinition serviceDefinition = new ServiceDefinition(serviceInterface, serviceName, 
        Collections.unmodifiableMap(methods));
    
    return serviceDefinition;
  }

  private String resolveServiceName(Class<?> serviceInterface, Service serviceAnnotation) {
    return Strings.isNullOrEmpty(serviceAnnotation.value()) ? serviceInterface.getName() : serviceAnnotation.value();
  }

  private String resolveMethodName(Method method, ServiceMethod methodAnnotation) {
    return Strings.isNullOrEmpty(methodAnnotation.value()) ? method.getName() : methodAnnotation.value();
  }
}
