package io.scalecube.services.gateway.client;

import io.scalecube.services.Address;
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

/**
 * Syntethic router for returning pre-constructed {@link ServiceReference} instance with given
 * address.
 */
public final class StaticAddressRouter implements Router {

  private final ServiceReference serviceReference;

  /**
   * Constructor.
   *
   * @param address address
   */
  public StaticAddressRouter(Address address) {
    serviceReference =
        new ServiceReference(
            new ServiceMethodDefinition(UUID.randomUUID().toString()),
            new ServiceRegistration(
                UUID.randomUUID().toString(), Collections.emptyMap(), Collections.emptyList()),
            ServiceEndpoint.builder().id(UUID.randomUUID().toString()).address(address).build());
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(serviceReference);
  }
}
