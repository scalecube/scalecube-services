package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
  public CompletableFuture<Message> invoke(final Message request) {
    checkArgument(request != null, "message can't be null");
    final Method method = this.methods.get(request.header(ServiceHeaders.METHOD));
    return invokeMethod(request, method);

  }

  private Object invoke(final Message request, final Method method)
      throws IllegalAccessException, InvocationTargetException {
    Object result;
    // handle invoke
    if (method.getParameters().length == 0) {
      result = method.invoke(serviceObject);
    } else if (method.getParameters()[0].getType().isAssignableFrom(Message.class)) {
      result = method.invoke(serviceObject, request);
    } else {
      result = method.invoke(serviceObject, new Object[] {request.data()});
    }
    return result;
  }

  @Override
  public Observable<Message> listen(Message request) {
    checkArgument(request != null, "message can't be null.");
    checkArgument(request.correlationId() != null, "subscribe request must contain correlationId.");

    final Method method = getMethod(request);
    checkArgument(method.getReturnType().equals(Observable.class), "subscribe method must return Observable.");

    final String cid = request.correlationId();
    try {
      final Observable<?> observable = (Observable<?>) invoke(request, method);
      return observable.map(message -> Messages.asResponse(message, cid));

    } catch (IllegalAccessException | InvocationTargetException ex) {
      return Observable.from(new Message[] {Messages.asResponse(ex, cid)});
    }
  }

  private CompletableFuture<Message> invokeMethod(final Message request, final Method method) {

    final CompletableFuture<Message> resultMessage = new CompletableFuture<>();
    try {
      final Object result = invoke(request, method);
      if (result instanceof CompletableFuture) {
        final CompletableFuture<?> resultFuture = (CompletableFuture<?>) result;
        resultFuture.whenComplete((success, error) -> {
          if (error == null) {
            if (Reflect.parameterizedReturnType(method).equals(Message.class)) {
              resultMessage.complete((Message) success);
            } else {
              resultMessage.complete(Messages.asResponse(success, request.correlationId()));
            }
          } else {
            resultMessage.completeExceptionally(error);
          }
        });
      }
    } catch (Exception ex) {
      resultMessage.completeExceptionally(ex);
    }

    return resultMessage;
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

  public Object serviceObject() {
    return this.serviceObject;
  }


  public Method getMethod(Message request) {
    return this.methods.get(request.header(ServiceHeaders.METHOD));
  }
}
