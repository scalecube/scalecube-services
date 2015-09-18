package io.servicefabric.services.annotations;

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.servicefabric.services.registry.ServiceInstance;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ServiceAnnotationsProcessor {

  // TODO [AK]: Add logging
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceAnnotationsProcessor.class);

  public static Collection<ServiceInstance> processService(Object serviceObject) {
    Class<?>[] serviceInterfaces = serviceObject.getClass().getInterfaces();
    ArrayList<ServiceInstance> serviceInstances = new ArrayList<>();
    for (Class<?> interfaceClazz : serviceInterfaces) {
      if (interfaceClazz.isAnnotationPresent(Service.class)) {
        Service serviceAnnotation = interfaceClazz.getAnnotation(Service.class);
        if (serviceAnnotation == null) {
          continue;
        }
        ServiceInstance serviceInstance = processService(serviceObject, interfaceClazz, serviceAnnotation);
        serviceInstances.add(serviceInstance);
      }
    }
    return serviceInstances;
  }

  private static ServiceInstance processService(Object serviceObject, Class<?> interfaceClazz, Service serviceAnnotation) {
    String serviceName = Strings.isNullOrEmpty(serviceAnnotation.value()) ? interfaceClazz.getName() : serviceAnnotation.value();
    Map<String, Method> methods = parseServiceMethods(serviceName, serviceObject, interfaceClazz);
    return new ServiceInstance(serviceObject, interfaceClazz, serviceName, methods);
  }

  private static Map<String, Method> parseServiceMethods(String serviceName, Object service, Class<?> serviceInterface) {
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
