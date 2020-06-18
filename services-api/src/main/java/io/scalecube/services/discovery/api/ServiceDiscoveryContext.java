package io.scalecube.services.discovery.api;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.Objects;
import java.util.StringJoiner;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public final class ServiceDiscoveryContext {

  private final String id;
  private final Address address;
  private final ServiceDiscovery discovery;
  private final ServiceRegistry serviceRegistry;
  private final Scheduler scheduler;

  private ServiceDiscoveryContext(Builder builder) {
    this.id = Objects.requireNonNull(builder.id, "id");
    this.address = Objects.requireNonNull(builder.address, "address");
    this.discovery = Objects.requireNonNull(builder.discovery, "discovery");
    this.serviceRegistry = Objects.requireNonNull(builder.serviceRegistry, "serviceRegistry");
    this.scheduler = Objects.requireNonNull(builder.scheduler, "scheduler");
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns copy builder.
   *
   * @param other instance to copy from
   * @return new builder instance
   */
  public static Builder from(ServiceDiscoveryContext other) {
    return new Builder()
        .id(other.id)
        .address(other.address)
        .discovery(other.discovery)
        .serviceRegistry(other.serviceRegistry)
        .scheduler(other.scheduler);
  }

  /**
   * Returns service discovery id.
   *
   * @return service discovery id
   */
  public String id() {
    return id;
  }

  /**
   * Returns service discovery address. If service discovery instance is not yet started, then this
   * method will return {@link Address#NULL_ADDRESS}.
   *
   * @see ServiceDiscovery#start()
   * @see ServiceDiscovery#listen()
   * @return service discovery address
   */
  public Address address() {
    return address;
  }

  /**
   * Returns stream of service discovery events. Can be called before or after {@link
   * ServiceDiscovery#start()}. If it's called before then new events will be streamed, if it's
   * called after then {@link ServiceRegistry#listServiceEndpoints()} will be turned to service
   * discovery events of type {@link Type#ENDPOINT_ADDED}, and concateneted with a stream of live
   * events.
   *
   * @return stream of service discovery events
   */
  public Flux<ServiceDiscoveryEvent> listen() {
    return Flux.fromStream(serviceRegistry.listServiceEndpoints().stream())
        .map(ServiceDiscoveryEvent::newEndpointAdded)
        .concatWith(discovery.listen())
        .subscribeOn(scheduler)
        .publishOn(scheduler);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceDiscoveryContext.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("address=" + address)
        .add("discovery=" + discovery)
        .add("serviceRegistry=" + serviceRegistry)
        .add("scheduler=" + scheduler)
        .toString();
  }

  public static class Builder {

    private String id;
    private Address address;
    private ServiceDiscovery discovery;
    private ServiceRegistry serviceRegistry;
    private Scheduler scheduler;

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

    public Builder serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public Builder scheduler(Scheduler scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public ServiceDiscoveryContext build() {
      return new ServiceDiscoveryContext(this);
    }
  }
}
