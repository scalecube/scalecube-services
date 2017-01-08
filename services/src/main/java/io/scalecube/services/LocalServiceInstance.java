package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

/**
 * Local service instance invokes the service instance hosted on this local process.
 * 
 *
 */
public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final Map<String, Method> methods;
  private final String serviceName;
  private final String memberId;
  private final Map<String, String> tags;
  private final Address address;

  /**
   * LocalServiceInstance instance constructor.
   * 
   * @param serviceConfig the instance of the service configurations.
   * @param memberId the Cluster memberId of this instance.
   * @param serviceName the qualifier name of the service.
   * @param methods the java methods of the service.
   */
  public LocalServiceInstance(ServiceConfig serviceConfig, Address address, String memberId, String serviceName,
      Map<String, Method> methods) {
    checkArgument(serviceConfig != null, "serviceConfig can't be null");
    checkArgument(serviceConfig.getService() != null, "serviceConfig.service can't be null");
    checkArgument(address != null, "address can't be null");
    checkArgument(memberId != null, "memberId can't be null");
    checkArgument(serviceName != null, "serviceName can't be null");
    checkArgument(methods != null, "methods can't be null");

    this.serviceObject = serviceConfig.getService();
    this.serviceName = serviceName;
    this.methods = methods;
    this.memberId = memberId;
    this.tags = serviceConfig.getTags();
    this.address = address;
  }


  @Override
  public Object invoke(Message message) throws Exception {
    checkArgument(message != null, "message can't be null");

    try {
      Method method = this.methods.get(message.header(ServiceHeaders.METHOD));
      Object result;

      if (method.getParameters().length == 0) {
        result = method.invoke(serviceObject);
      } else if (method.getParameters()[0].getType().isAssignableFrom(Message.class)) {
        result = method.invoke(serviceObject, message);
      } else {
        result = method.invoke(serviceObject, new Object[] {message.data()});
      }
      return result;
    } catch (Exception ex) {
      return ex;
    }
  }

  public String serviceName() {
    return serviceName;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  @Override
  public Boolean isLocal() {
    return true;
  }

  @Override
  public String toString() {
    return "LocalServiceInstance [serviceObject=" + serviceObject + ", memberId=" + memberId + "]";
  }


  @Override
  public Map<String, String> tags() {
    return Collections.unmodifiableMap(tags);
  }


  @Override
  public Address address() {
    return this.address;
  }
}
