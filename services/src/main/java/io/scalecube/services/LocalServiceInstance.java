package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Message;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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

  /**
   * LocalServiceInstance instance constructor.
   * 
   * @param serviceObject the instance of the service object.
   * @param memberId the Cluster memberId of this instance.
   * @param serviceName the qualifier name of the service.
   * @param methods the java methods of the service.
   * @param tags service tags of this instance.
   */
  public LocalServiceInstance(Object serviceObject, String memberId, String serviceName, Map<String, Method> methods,
      Tag[] tags) {
    checkArgument(serviceObject != null);
    checkArgument(memberId != null);
    checkArgument(serviceName != null);
    checkArgument(methods != null);
    this.serviceObject = serviceObject;
    this.serviceName = serviceName;
    this.methods = methods;
    this.memberId = memberId;
    this.tags = toMap(tags);
  }

  private Map<String, String> toMap(Tag[] tags) {
    return Arrays.stream(tags).map(tag -> tag)
        .collect(Collectors.toMap(tag -> tag.getKey(), tag -> tag.getValue()));
  }

  @Override
  public Object invoke(Message message, ServiceDefinition definition) throws Exception {
    checkArgument(message != null);

    try {
      Method method = this.methods.get(message.header(ServiceHeaders.METHOD));
      Object result;

      if (method.getParameters().length == 0) {
        result = method.invoke(serviceObject);
      } else if (method.getParameters()[0].getType().isAssignableFrom(Message.class)) {
        if (message.data().getClass().isAssignableFrom(Message.class)) {
          result = method.invoke(serviceObject, (Message) message.data());
        } else {
          result = method.invoke(serviceObject, message);
        }
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
  public Map<String, String> tags() {
    return tags;
  }

  @Override
  public String toString() {
    return "LocalServiceInstance [serviceName=" + serviceName + ", memberId=" + memberId + ", tags=" + tags + "]";
  }
}
