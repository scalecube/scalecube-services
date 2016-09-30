package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.scalecube.transport.Message;

public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final ServiceDefinition serviceDefinition;
  private final String memberId;
  
  public LocalServiceInstance(Object serviceObject, ServiceDefinition serviceDefinition, String memberId) {
    checkArgument(serviceObject != null);
    checkArgument(serviceDefinition != null);
    this.serviceObject = serviceObject;
    this.serviceDefinition = serviceDefinition;
    this.memberId = memberId;
  }

  public String serviceName() {
    return serviceDefinition.serviceName();
  }

  public Object invoke(String methodName, Message data) throws InvocationTargetException, IllegalAccessException {
    // TODO: safety checks
    // TODO: consider to return ListenableFuture (result, immediate or failed with corresponding exceptions)
    Method method = serviceDefinition.method();
    
    checkArgument(method != null, "Unknown method name %s", methodName);
    Object result = method.invoke(serviceObject, data);
    return result;
  }

  @Override
  public Object memberId() {
    return this.memberId;
  }

}
