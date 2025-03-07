package io.scalecube.services.routing;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Syntethic router for returning pre-constructed {@link ServiceReference} instance on the given
 * address.
 */
public class StaticAddressRouter implements Router {

  private final ServiceReference serviceReference;

  private StaticAddressRouter(Builder builder) {
    Objects.requireNonNull(builder.address, "builder.address");
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

  public static Builder from(Address address) {
    return new Builder().address(address);
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

    /**
     * Setter for {@code address}. This address will be returned in the {@link
     * StaticAddressRouter#route(ServiceRegistry, ServiceMessage)}.
     *
     * @param address address
     * @return this
     */
    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    /**
     * Setter for whether to apply behavior of {@link ClientTransport.CredentialsSupplier}, or not.
     * If it is known upfront that destination service is secured, then set this flag to {@code
     * true}, in such case {@link CredentialsSupplier#credentials(ServiceReference, String)} will be
     * invoked.
     *
     * @param secured secured flag
     * @return this
     */
    public Builder secured(boolean secured) {
      isSecured = secured;
      return this;
    }

    /**
     * Setter for {@code serviceRole} property, will be used in the invocation of {@link
     * CredentialsSupplier#credentials(ServiceReference, String)}.
     *
     * @param serviceRole serviceRole
     * @return this
     */
    public Builder serviceRole(String serviceRole) {
      this.serviceRole = serviceRole;
      return this;
    }

    public StaticAddressRouter build() {
      return new StaticAddressRouter(this);
    }
  }
}
