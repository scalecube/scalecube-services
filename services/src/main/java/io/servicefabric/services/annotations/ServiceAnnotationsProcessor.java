package io.servicefabric.services.annotations;

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class ServiceAnnotationsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceAnnotationsProcessor.class);

  //private IServiceRegistry serviceRegistry;

  //private IServiceMethodRegistry serviceMethodRegistry;


  public void registerService(Object service) {
    Class<?>[] serviceInterfaces = service.getClass().getInterfaces();
    for (Class<?> interfaceClazz : serviceInterfaces) {
      if (interfaceClazz.isAnnotationPresent(Service.class)) {
        Service serviceAnnotation = interfaceClazz.getAnnotation(Service.class);
        if (serviceAnnotation == null) {
          continue;
        }
        registerService(service, interfaceClazz, serviceAnnotation);
      }
    }
  }

  private void registerService(Object service, Class<?> interfaceClazz, Service serviceAnnotation) {
    String serviceName = Strings.isNullOrEmpty(serviceAnnotation.value()) ? interfaceClazz.getName() : serviceAnnotation.value();

    //serviceRegistry.registerService(serviceName); //TODO ??

    registerServiceMethods(serviceName, service, interfaceClazz);
  }

  public void registerServiceMethods(String serviceName, Object service, Class<?> serviceInterface) {
    for (Method method : serviceInterface.getMethods()) {
      if (method.isAnnotationPresent(ServiceMethod.class)) {
        ServiceMethod serviceMethodAnnotation = method.getAnnotation(ServiceMethod.class);
        String methodName = Strings.isNullOrEmpty(serviceMethodAnnotation.value()) ? method.getName() :
            serviceMethodAnnotation.value();

        // serviceMethodRegistry.registerActionServiceMethod(qualifier, service, method); //TODO ??

        LOGGER.debug("Registered method '{}' for service '{}': {}", methodName, serviceName, service);
      }
    }
  }


}
