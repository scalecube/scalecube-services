package io.scalecube.services.routing;

import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.services.registry.api.ServiceRegistry;
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
    Objects.requireNonNull(builder.serviceName, "builder.serviceName");
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
                .name(builder.serviceName)
                .address(builder.address)
                .build());
  }

  /**
   * Creates {@link Builder} with service address and service name.
   *
   * @param address service address
   * @param name logical service name
   * @return builder instance
   */
  public static Builder forService(Address address, String name) {
    return new Builder().address(address).serviceName(name);
  }

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return Optional.of(serviceReference);
  }

  public static class Builder {

    private Address address;
    private boolean isSecured;
    private String serviceRole;
    private String serviceName;

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
     * Setter for whether to apply behavior of {@link CredentialsSupplier}, or not. If it is known
     * upfront that destination service is secured, then set this flag to {@code true}, in such case
     * {@link CredentialsSupplier#credentials(String)} will be invoked.
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
     * CredentialsSupplier#credentials(String)}.
     *
     * @param serviceRole serviceRole
     * @return this
     */
    public Builder serviceRole(String serviceRole) {
      this.serviceRole = serviceRole;
      return this;
    }

    /**
     * Setter for {@code serviceName} property, will be used in the invocation of {@link
     * CredentialsSupplier#credentials(String)}.
     *
     * @param serviceName serviceName
     * @return this
     */
    public Builder serviceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public StaticAddressRouter build() {
      return new StaticAddressRouter(this);
    }
  }
}
