package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.streams.StreamMessage;
import io.scalecube.transport.Address;

import rx.Observable;

import java.lang.reflect.Method;
import java.util.Collection;
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
  private final Metrics metrics;

  /**
   * LocalServiceInstance instance constructor.
   * 
   * @param serviceObject the instance of the service configurations.
   * @param memberId the Cluster memberId of this instance.
   * @param serviceName the qualifier name of the service.
   * @param methods the java methods of the service.
   * @param metrics factory measuring service kpis
   */
  public LocalServiceInstance(Object serviceObject,
      Map<String, String> tags,
      Address address, String memberId, String serviceName,
      Map<String, Method> methods, Metrics metrics) {
    checkArgument(serviceObject != null, "serviceObject can't be null");
    checkArgument(address != null, "address can't be null");
    checkArgument(memberId != null, "memberId can't be null");
    checkArgument(serviceName != null, "serviceName can't be null");
    checkArgument(methods != null, "methods can't be null");

    this.serviceObject = serviceObject;
    this.serviceName = serviceName;
    this.methods = Collections.unmodifiableMap(methods);
    this.tags = Collections.unmodifiableMap(tags);
    this.memberId = memberId;
    this.address = address;
    this.metrics = metrics;

  }

  public LocalServiceInstance(ServiceConfig serviceConfig, Address address, String memberId, String serviceName,
      Map<String, Method> method) {
    this(serviceConfig.getService(), serviceConfig.getTags(), address, memberId, serviceName, method, null);
  }

  @Override
  public CompletableFuture<StreamMessage> invoke(final StreamMessage request) {
    return invoke(request, StreamMessage.class);
  }

  @Override
  public <T> CompletableFuture<StreamMessage> invoke(StreamMessage request, Class<T> responseType) {
    checkArgument(request != null, "message can't be null");

    final Method method = this.methods.get(Messages.qualifierOf(request).getAction());
    return invokeMethod(request, method);
  }

  @Override
  public Observable<StreamMessage> listen(StreamMessage request) {
    return listen(request, StreamMessage.class);
  }

  @Override
  public <T> Observable<T> listen(StreamMessage request, Class<T> responseType) {
    checkArgument(request != null, "message can't be null.");
    final Method method = getMethod(request);
    checkArgument(method.getReturnType().equals(Observable.class), "subscribe method must return Observable.");
    Observable<T> observable = null;
    try {
      observable = Reflect.invoke(serviceObject, method, request);
      return observable.map(message -> {
        Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "onNext");
        return message;
      });

    } catch (Exception ex) {
      Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "error");

      return Observable.error(ex);
    }
  }

  private CompletableFuture<StreamMessage> invokeMethod(final StreamMessage request, final Method method) {
    final CompletableFuture<StreamMessage> resultMessage = new CompletableFuture<>();
    try {
      Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "request");
      final Object result = Reflect.invoke(this.serviceObject, method, request);
      if (result instanceof CompletableFuture) {
        final CompletableFuture<?> resultFuture = (CompletableFuture<?>) result;
        resultFuture.whenComplete((success, error) -> {
          if (error == null) {
            Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "response");
            if (Reflect.parameterizedReturnType(method).equals(StreamMessage.class)) {
              resultMessage.complete((StreamMessage) success);
            } else {
              resultMessage.complete(StreamMessage.from(request).data(success).build());
            }
          } else {
            Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "error");
            resultMessage.completeExceptionally(error);
          }
        });
      } else if (result == null) {
        resultMessage.complete(StreamMessage.from(request).data(null).build());
      }
    } catch (Exception ex) {
      Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "exception");
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


  public Method getMethod(StreamMessage request) {
    return this.methods.get(Messages.qualifierOf(request).getAction());
  }

  @Override
  public boolean methodExists(String methodName) {
    return methods.containsKey(methodName);
  }


  @Override
  public void checkMethodExists(String methodName) {
    checkArgument(methodExists(methodName), "instance has no such requested method");
  }

  @Override
  public Collection<String> methods() {
    return methods.keySet();
  }
}
