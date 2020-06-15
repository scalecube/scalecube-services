package io.scalecube.services.discovery.api;

import io.scalecube.net.Address;
import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Flux;

public final class ServiceDiscoveryContext {

  private final String id;
  private final Address address;
  private final ServiceDiscovery discovery;

  private ServiceDiscoveryContext(Builder builder) {
    this.id = Objects.requireNonNull(builder.id, "discoveryContext.id");
    this.address = Objects.requireNonNull(builder.address, "discoveryContext.address");
    this.discovery = Objects.requireNonNull(builder.discovery, "discoveryContext.discovery");
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return id;
  }

  public Address address() {
    return address;
  }

  public Flux<ServiceDiscoveryEvent> listen() {
    return discovery.listen();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceDiscoveryContext.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("address=" + address)
        .add("discovery=" + discovery)
        .toString();
  }

  public static class Builder {

    private String id;
    private Address address;
    private ServiceDiscovery discovery;

    private Builder() {}

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Builder discovery(ServiceDiscovery discovery) {
      this.discovery = discovery;
      return this;
    }

    public ServiceDiscoveryContext build() {
      return new ServiceDiscoveryContext(this);
    }
  }
}
