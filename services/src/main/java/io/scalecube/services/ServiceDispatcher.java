package io.scalecube.services;

import static io.scalecube.services.ServiceHeaders.serviceMethod;
import static io.scalecube.services.ServiceHeaders.serviceRequest;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Message;

import java.util.Optional;

public class ServiceDispatcher {

  private final ServiceCommunicator cluster;
  private final ServiceRegistry registry;

  /**
   * ServiceDispatcher constructor to listen on incoming network service request.
   * 
   * @param cluster instance to listen on events.
   * @param registry service registry instance for dispatching.
   */
  public ServiceDispatcher(ServiceCommunicator cluster, ServiceRegistry registry) {
    this.cluster = cluster;
    this.registry = registry;


    // Start listen messages
    cluster.listen()
        .filter(message -> serviceRequest(message) != null)
        .subscribe(this::onServiceRequest);
  }

  private void onServiceRequest(final Message request) {
    Optional<ServiceInstance> serviceInstance =
        registry.getLocalInstance(serviceRequest(request), serviceMethod(request));

    DispatchingFuture result = DispatchingFuture.from(cluster, request);
    try {
      if (serviceInstance.isPresent()) {
        result.complete(serviceInstance.get().invoke(request));
      } else {
        result.completeExceptionally(new IllegalStateException("Service instance is missing: " + request.qualifier()));
      }
    } catch (Exception ex) {
      result.completeExceptionally(ex);
    }
  }
}
