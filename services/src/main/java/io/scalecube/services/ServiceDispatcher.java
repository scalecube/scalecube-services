package io.scalecube.services;

import static io.scalecube.services.ServiceHeaders.serviceMethod;
import static io.scalecube.services.ServiceHeaders.serviceRequest;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Message;

import rx.Observable;
import rx.Subscription;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ServiceDispatcher {

  private final ServiceCommunicator sender;
  private final ServiceRegistry registry;

  /**
   * ServiceDispatcher constructor to listen on incoming network service request.
   * 
   * @param sender instance to listen on events.
   * @param registry service registry instance for dispatching.
   */
  public ServiceDispatcher(ServiceCommunicator sender, ServiceRegistry registry) {
    this.sender = sender;
    this.registry = registry;

    // Start listen messages
    sender.listen()
        .filter(message -> serviceRequest(message) != null)
        .subscribe(this::onServiceRequest);
  }

  private void onServiceRequest(final Message request) {
    Optional<ServiceInstance> serviceInstance =
        registry.getLocalInstance(serviceRequest(request), serviceMethod(request));

    DispatchingFuture result = DispatchingFuture.from(sender, request);
    try {
      if (serviceInstance.isPresent() && serviceInstance.get() instanceof LocalServiceInstance) {
        LocalServiceInstance instance = (LocalServiceInstance) serviceInstance.get();
        Method method = instance.getMethod(request);
        if (method.getReturnType().equals(CompletableFuture.class)) {
          result.complete(serviceInstance.get().invoke(request));
        
        } else if (method.getReturnType().equals(Observable.class)) {
          Subscription subscription = serviceInstance.get().listen(request)
              .subscribe(onNext -> {
                sender.send(request.sender(), onNext);
              });
          
        }
        
      } else {
        result.completeExceptionally(new IllegalStateException("Service instance is missing: " + request.qualifier()));
      }
    } catch (Exception ex) {
      result.completeExceptionally(ex);
    }
  }
}
