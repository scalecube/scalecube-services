package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceReference;
import io.scalecube.transport.Address;

public class ReferenceRegistryEvent {

  private ServiceReference serviceReference;
  private RegistryEventType type;

  public static ReferenceRegistryEvent createAdded(ServiceReference serviceReference) {
    return new ReferenceRegistryEvent(RegistryEventType.ADDED, serviceReference);
  }

  public static ReferenceRegistryEvent createRemoved(ServiceReference serviceReference) {
    return new ReferenceRegistryEvent(RegistryEventType.REMOVED, serviceReference);
  }

  private ReferenceRegistryEvent(RegistryEventType type, ServiceReference serviceReference) {
    this.serviceReference = serviceReference;
    this.type = type;
  }

  private ReferenceRegistryEvent(ReferenceRegistryEvent e) {
    this.serviceReference = e.serviceReference;
    this.type = e.type;
  }

  public ServiceReference serviceReference() {
    return this.serviceReference;
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
    return Address.create(this.serviceReference.host(), this.serviceReference.port());
  }
}
