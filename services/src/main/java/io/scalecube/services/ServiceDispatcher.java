package io.scalecube.services;

import static io.scalecube.services.ServiceHeaders.service_method_of;
import static io.scalecube.services.ServiceHeaders.service_request_of;

import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;

import java.util.Optional;

public class ServiceDispatcher {

  private final ITransport transport;
  private final ServiceRegistry registry;

  /**
   * ServiceDispatcher constructor to listen on incoming network service request.
   * 
   * @param transport instance to listen on events.
   * @param registry service registry instance for dispatching.
   */
  public ServiceDispatcher(ITransport transport, ServiceRegistry registry) {
    this.transport = transport;
    this.registry = registry;


    // Start listen messages
    transport.listen().subscribe(this::onServiceRequest);
  }

  private void onServiceRequest(final Message request) {
    if(service_request_of(request) != null) {
      Optional<ServiceInstance> serviceInstance =
          registry.getLocalInstance(service_request_of(request), service_method_of(request));
  
      DispatchingFuture result = DispatchingFuture.from(transport, request);
      try {
        if (serviceInstance.isPresent()) {
          result.complete(serviceInstance.get().invoke(request, null));
        } else {
          result.completeExceptionally(new IllegalStateException("Service instance is missing: " + request.qualifier()));
        }
      } catch (Exception ex) {
        result.completeExceptionally(ex);
      }
    }
  }
}
