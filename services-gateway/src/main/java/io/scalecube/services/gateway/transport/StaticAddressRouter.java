package io.scalecube.services.gateway.transport;

import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

/** Syntethic router for returning preconstructed static service reference with given address. */
public class StaticAddressRouter implements Router {

  private final ServiceReference staticServiceReference;

  /**
   * Constructor.
   *
   * @param address address
   */
  public StaticAddressRouter(Address address) {
    this.staticServiceReference =
        new ServiceReference(
            new ServiceMethodDefinition(UUID.randomUUID().toString()),
            new ServiceRegistration(
                UUID.randomUUID().toString(), Collections.emptyMap(), Collections.emptyList()),
            ServiceEndpoint.builder().id(UUID.randomUUID().toString()).addresses(address).build());
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(staticServiceReference);
  }
}
