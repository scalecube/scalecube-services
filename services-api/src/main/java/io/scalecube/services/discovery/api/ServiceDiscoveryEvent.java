package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

/**
 * Service registration event. This event is being fired when {@link ServiceEndpoint} is being added
 * (or removed from) to (from) {@link ServiceRegistry}.
 */
public class ServiceDiscoveryEvent {

  public enum Type {
    REGISTERED, // service endpoint added
    UNREGISTERED // service endpoint removed
  }

  private final ServiceEndpoint serviceEndpoint;
  private final Type type;

  private ServiceDiscoveryEvent(ServiceEndpoint serviceEndpoint, Type type) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public static ServiceDiscoveryEvent registered(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(serviceEndpoint, Type.REGISTERED);
  }

  public static ServiceDiscoveryEvent unregistered(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(serviceEndpoint, Type.UNREGISTERED);
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public Type type() {
    return this.type;
  }

  public boolean isRegistered() {
    return Type.REGISTERED.equals(this.type);
  }

  public boolean isUnregistered() {
    return Type.UNREGISTERED.equals(this.type);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ServiceDiscoveryEvent{");
    sb.append("serviceEndpoint=").append(serviceEndpoint);
    sb.append(", type=").append(type);
    sb.append('}');
    return sb.toString();
  }
}
