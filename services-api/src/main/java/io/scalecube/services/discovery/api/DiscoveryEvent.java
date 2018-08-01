package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

/**
 * Service registration event. This event is being fired when {@link ServiceEndpoint} is being added (or removed from)
 * to (from) {@link ServiceRegistry}.
 */
public class DiscoveryEvent {

  public enum Type {
    REGISTERED, // service endpoint added
    UNREGISTERED // service endpoint removed
  }

  private final ServiceEndpoint serviceEndpoint;
  private final Type type;

  private DiscoveryEvent(Type type, ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public static DiscoveryEvent registered(ServiceEndpoint serviceEndpoint) {
    return new DiscoveryEvent(Type.REGISTERED, serviceEndpoint);
  }

  public static DiscoveryEvent unregistered(ServiceEndpoint serviceEndpoint) {
    return new DiscoveryEvent(Type.UNREGISTERED, serviceEndpoint);
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public Type type() {
    return this.type;
  }

  @Override
  public String toString() {
    return "RegistrationEvent [serviceEndpoint=" + serviceEndpoint + ", type=" + type + "]";
  }

  public boolean isRegistered() {
    return Type.REGISTERED.equals(this.type);
  }

  public boolean isUnregistered() {
    return Type.UNREGISTERED.equals(this.type);
  }


}
