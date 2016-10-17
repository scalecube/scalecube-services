package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.scalecube.transport.Message;

public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final ServiceDefinition serviceDefinition;
  private final String memberId;
  private final Boolean isLocal;
  
  public LocalServiceInstance(Object serviceObject, ServiceDefinition serviceDefinition, String memberId) {
    checkArgument(serviceObject != null);
    checkArgument(serviceDefinition != null);
    this.serviceObject = serviceObject;
    this.serviceDefinition = serviceDefinition;
    this.memberId = memberId;
    this.isLocal = true;
  }

  public String qualifier() {
    return serviceDefinition.qualifier();
  }

  public Object invoke(Message message) throws InvocationTargetException, IllegalAccessException {
    // TODO: safety checks
    // TODO: consider to return ListenableFuture (result, immediate or failed with corresponding exceptions)
    Method method = serviceDefinition.method();
    
    checkArgument(method != null, "Unknown method name %s", method.getName());
    Object result = null;
    if(method.getParameters().length > 0 && method.getParameters()[0].getType().equals(Message.class)){
      result = method.invoke(serviceObject, message);
    }else if(method.getParameters().length > 0){
      result = method.invoke(serviceObject,  new Object[]{message.data()});
    }else{
      result = method.invoke(serviceObject);
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

}
