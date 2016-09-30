package io.scalecube.services.annotations;

import com.google.common.base.Strings;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceProcessor;

import static com.google.common.base.Preconditions.*;

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
  public ConcurrentMap<String, ServiceDefinition> introspectServiceInterface(Class<?> serviceInterface) {
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    checkArgument(serviceAnnotation != null, "Not a service interface: %s", serviceInterface);
    String serviceName = Strings.isNullOrEmpty(serviceAnnotation.value()) ? serviceInterface.getName() : serviceAnnotation.value();
    Map<String, Method> methods = parseServiceMethods(serviceInterface);
    
    ConcurrentMap<String, ServiceDefinition> serviceDefinitions = new ConcurrentHashMap();
    
    for(Entry<String, Method> method : methods.entrySet()){
      serviceDefinitions.put(method.getKey(), new ServiceDefinition(serviceInterface, serviceName + "/" + method.getKey(), method.getValue()));
    }
    return serviceDefinitions;
  }

  private Map<String, Method> parseServiceMethods(Class<?> serviceInterface) {
    Map<String, Method> methods = new HashMap<>();
    for (Method method : serviceInterface.getMethods()) {
      if (method.isAnnotationPresent(ServiceMethod.class)) {
        ServiceMethod serviceMethodAnnotation = method.getAnnotation(ServiceMethod.class);
        String methodName = Strings.isNullOrEmpty(serviceMethodAnnotation.value()) ? method.getName() :
            serviceMethodAnnotation.value();
        if (methods.containsKey(methodName)) {
          throw new IllegalStateException("Service method with name " + methodName + " already exists");
        }
        methods.put(methodName, method);
      }
    }
    return methods;
  }


}
