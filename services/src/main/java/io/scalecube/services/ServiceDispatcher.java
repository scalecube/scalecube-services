package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import java.util.Optional;

public class ServiceDispatcher {

  private final ICluster cluster;
  private final ServiceRegistry registry;

  /**
   * ServiceDispatcher constructor to listen on incoming network service request.
   * 
   * @param cluster instance to listen on events.
   * @param registry service registry instance for dispatching.
   */
  public ServiceDispatcher(ICluster cluster, ServiceRegistry registry) {
    this.cluster = cluster;
    this.registry = registry;

    // Start listen messages
    cluster.listen()
        .filter(message -> message.qualifier() != null)
        .subscribe(this::onServiceRequest);
  }

  private void onServiceRequest(final Message request) {
    Optional<ServiceInstance> serviceInstance =
        registry.getLocalInstance(request.qualifier(), request.header(ServiceHeaders.METHOD));

    DispatchingFuture result = DispatchingFuture.from(cluster, request);
    try {
      result.complete(serviceInstance.get().invoke(request, null));
    } catch (Exception ex) {
      result.completeExceptionally(ex);
    }
  }
}
