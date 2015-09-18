package io.servicefabric.services.annotations;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Anton Kharenko
 */
public class ServiceInstance {

  private final Object serviceObject;
  private final Class<?> interfaceClass;
  private final String serviceName;
  private final Map<String, Method> methods;

  public ServiceInstance(Object serviceObject, Class<?> interfaceClass, String serviceName, Map<String, Method> methods) {
    checkArgument(serviceObject != null);
    checkArgument(interfaceClass != null);
    checkArgument(serviceName != null);
    checkArgument(methods != null && !methods.isEmpty());
    this.serviceObject = serviceObject;
    this.interfaceClass = interfaceClass;
    this.serviceName = serviceName;
    this.methods = Collections.unmodifiableMap(methods);
  }

  public Object getServiceObject() {
    return serviceObject;
  }

  public Class<?> getInterfaceClass() {
    return interfaceClass;
  }

  public String getServiceName() {
    return serviceName;
  }

  public Set<String> getServiceMethods() {
    return methods.keySet();
  }

  public Object invoke(String methodName, Object data) throws InvocationTargetException, IllegalAccessException {
    // TODO: safety checks
    // TODO: consider to return ListenableFuture (result, immediate or failed with corresponding exceptions)
    Method m = methods.get(methodName);
    checkArgument(m != null, "Unknown method name %s", methodName);
    Object result = m.invoke(serviceObject, data);
    return result;
  }

}
