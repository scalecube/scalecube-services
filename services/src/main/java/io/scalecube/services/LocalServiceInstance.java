package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.scalecube.transport.Message;

public class LocalServiceInstance implements ServiceInstance {

  @Override
  public String toString() {
    return "LocalServiceInstance [serviceObject=" + serviceObject + ", memberId=" + memberId + ", isLocal=" + isLocal + "]";
  }

  private final Object serviceObject;
  private final String memberId;
  
  private final String qualifier;
  private final Method method;
  
  private final Boolean isLocal;
  private String[] tags;
  
  public LocalServiceInstance(Object serviceObject, String memberId, Class<?> serviceInterface, 
      String qualifier, 
      Method method, 
      String[] tags) {
    
    checkArgument(serviceObject != null);
    this.serviceObject = serviceObject;
    this.memberId = memberId;
    this.isLocal = true;
    this.qualifier = qualifier;
    this.method = method;
    this.tags = tags;
  }

  public String[] tags() {
    return tags;
  }
  
  public String qualifier() {
    return qualifier;
  }

  public <T> Object invoke(Message message, Class<T> returnType) throws InvocationTargetException, IllegalAccessException {
    // TODO: safety checks
    // TODO: consider to return ListenableFuture (result, immediate or failed with corresponding exceptions)
    Method method = this.method;
    
    Object result = null;
    
    if (method.getParameters().length == 0) {
      result = method.invoke(serviceObject); 
    } else if (method.getParameters().length > 0 && method.getParameters()[0].getType().equals(Message.class)){
      result = method.invoke(serviceObject, message);
    }else { 
      result = method.invoke(serviceObject,  new Object[]{message.data()}); 
    } 
    return result;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  @Override
  public Boolean isLocal() {
    return isLocal;
  }

  @Override
  public boolean isReachable() {
    return true;
  }
}
