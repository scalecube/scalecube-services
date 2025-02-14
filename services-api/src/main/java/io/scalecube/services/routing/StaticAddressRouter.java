package io.scalecube.services.routing;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Syntethic router for returning pre-constructed {@link ServiceReference} instance with given
 * address.
 */
public class StaticAddressRouter implements Router {

  private final ServiceReference serviceReference;

  private StaticAddressRouter(Builder builder) {
    serviceReference =
        new ServiceReference(
            ServiceMethodDefinition.builder()
                .secured(builder.isSecured)
                .allowedRoles(
                    builder.serviceRole != null ? List.of(builder.serviceRole) : List.of())
                .build(),
            new ServiceRegistration(
                UUID.randomUUID().toString(), Collections.emptyMap(), Collections.emptyList()),
            ServiceEndpoint.builder()
                .id(UUID.randomUUID().toString())
                .address(builder.address)
                .build());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static StaticAddressRouter fromAddress(Address address) {
    return builder().address(address).build();
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(serviceReference);
  }

  public static class Builder {

    private Address address;
    private boolean isSecured;
    private String serviceRole;

    private Builder() {}

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Builder secured(boolean secured) {
      isSecured = secured;
      return this;
    }

    public Builder serviceRole(String serviceRole) {
      this.serviceRole = serviceRole;
      return this;
    }

    public StaticAddressRouter build() {
      return new StaticAddressRouter(this);
    }
  }
}
