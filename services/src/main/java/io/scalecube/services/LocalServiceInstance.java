package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.api.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.transport.Address;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import reactor.core.publisher.Flux;

/**
 * Local service instance invokes the service instance hosted on this local process.
 * 
 *
 */
public class LocalServiceInstance extends ServiceInstance {

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
    requireNonNull(serviceObject != null, "serviceObject can't be null");
    requireNonNull(address != null, "address can't be null");
    requireNonNull(memberId != null, "memberId can't be null");
    requireNonNull(serviceName != null, "serviceName can't be null");
    requireNonNull(methods != null, "methods can't be null");

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
  public CompletableFuture<ServiceMessage> invoke(ServiceMessage request) {
    requireNonNull(request != null, "message can't be null");
    requireNonNull(request.qualifier() != null, "message.qualifier can't be null");

    final Method method = this.methods.get(Messages.qualifierOf(request).getAction());
    return invokeMethod(request, method);
  }

  @Override
  public Flux<ServiceMessage> listen(ServiceMessage request) {
    requireNonNull(request != null, "message can't be null.");
    final Method method = getMethod(request);
    requireNonNull(method.getReturnType().equals(Flux.class), "listen method must return Flux.");
    Flux<ServiceMessage> flux = null;
    try {
      flux = Reflect.invoke(serviceObject, method, request);
      return flux.map(message -> {
        Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "onNext");
        return message;
      });

    } catch (Exception ex) {
      Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "error");
      return flux.error(ex);
    }
  }

  private CompletableFuture<ServiceMessage> invokeMethod(final ServiceMessage request, final Method method) {
    final CompletableFuture<ServiceMessage> resultMessage = new CompletableFuture<>();
    try {
      Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "request");
      final Object result = Reflect.invoke(this.serviceObject, method, request);
      
      if (result instanceof CompletableFuture) {
        final CompletableFuture<?> resultFuture = (CompletableFuture<?>) result;
        resultFuture.whenComplete((success, error) -> {
          if (error == null) {
            Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "response");
            if (Reflect.parameterizedReturnType(method).equals(ServiceMessage.class)) {
              resultMessage.complete((ServiceMessage) success);
            } else {
              resultMessage.complete(ServiceMessage.from(request).data(success).build());
            }
          } else {
            Metrics.mark(metrics, this.serviceObject.getClass(), method.getName(), "error");
            resultMessage.completeExceptionally(error);
          }
        });
      } else if (result == null) {
        resultMessage.complete(ServiceMessage.from(request).data(null).build());
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


  public Method getMethod(ServiceMessage request) {
    return this.methods.get(Messages.qualifierOf(request).getAction());
  }

  @Override
  public boolean methodExists(String methodName) {
    return methods.containsKey(methodName);
  }


  @Override
  public void checkMethodExists(String methodName) {
    requireNonNull(methodExists(methodName), "instance has no such requested method");
  }

  @Override
  public Collection<String> methods() {
    return methods.keySet();
  }
}
