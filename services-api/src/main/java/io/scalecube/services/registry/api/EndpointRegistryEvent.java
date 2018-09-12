package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.transport.Address;

public class EndpointRegistryEvent {

  private ServiceEndpoint serviceEndpoint;
  private RegistryEventType type;

  public static EndpointRegistryEvent createAdded(ServiceEndpoint serviceEndpoint) {
    return new EndpointRegistryEvent(RegistryEventType.ADDED, serviceEndpoint);
  }

  public static EndpointRegistryEvent createRemoved(ServiceEndpoint serviceEndpoint) {
    return new EndpointRegistryEvent(RegistryEventType.REMOVED, serviceEndpoint);
  }

  private EndpointRegistryEvent(RegistryEventType type, ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  private EndpointRegistryEvent(EndpointRegistryEvent e) {
    this.serviceEndpoint = e.serviceEndpoint;
    this.type = e.type;
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public boolean isAdded() {
    return RegistryEventType.ADDED.equals(type);
  }

  public boolean isRemoved() {
    return RegistryEventType.REMOVED.equals(type);
  }

  public RegistryEventType type() {
    return this.type;
  }

  public Address address() {
    return Address.create(this.serviceEndpoint.host(), this.serviceEndpoint.port());
  }
}
