package io.scalecube.services.discovery.api;

public class ServiceGroupDiscoveryEvent {

  public enum Type {
    REGISTERED, // service endpoint group added
    UNREGISTERED // service endpoint group removed
  }

  private final Type type;
  private final ServiceGroup endpointGroup;

  private ServiceGroupDiscoveryEvent(Type type, ServiceGroup endpointGroup) {
    this.type = type;
    this.endpointGroup = endpointGroup;
  }

  public static ServiceGroupDiscoveryEvent registered(ServiceGroup endpointGroup) {
    return new ServiceGroupDiscoveryEvent(Type.REGISTERED, endpointGroup);
  }

  public static ServiceGroupDiscoveryEvent unregistered(ServiceGroup endpointGroup) {
    return new ServiceGroupDiscoveryEvent(Type.UNREGISTERED, endpointGroup);
  }

  public boolean isRegistered() {
    return Type.REGISTERED.equals(this.type);
  }

  public boolean isUnregistered() {
    return Type.UNREGISTERED.equals(this.type);
  }

  public ServiceGroup endpointGroup() {
    return endpointGroup;
  }

  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return "ServiceGroupDiscoveryEvent{"
        + "type="
        + type
        + ", endpointGroup="
        + endpointGroup
        + '}';
  }
}
