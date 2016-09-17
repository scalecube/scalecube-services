package io.scalecube.services;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import io.scalecube.transport.Message;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Anton Kharenko
 */
public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final ServiceDefinition serviceDefinition;

  public LocalServiceInstance(Object serviceObject, ServiceDefinition serviceDefinition) {
    checkArgument(serviceObject != null);
    checkArgument(serviceDefinition != null);
    this.serviceObject = serviceObject;
    this.serviceDefinition = serviceDefinition;
  }

  public String serviceName() {
    return serviceDefinition.serviceName();
  }

  public Set<String> methodNames() {
    return serviceDefinition.methodNames();
  }

  public Object invoke(String methodName, Message data) throws InvocationTargetException, IllegalAccessException {
    // TODO: safety checks
    // TODO: consider to return ListenableFuture (result, immediate or failed with corresponding exceptions)
    Method method = serviceDefinition.method(methodName);
    checkArgument(method != null, "Unknown method name %s", methodName);
    Object result = method.invoke(serviceObject, data);
    return result;
}

}
