package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;

/**
 * This event is being fired from {@link ServiceDiscovery} when it detects that service endpoint was
 * being added or removed to/from cluster.
 */
public class ServiceDiscoveryEvent {

  public enum Type {
    ENDPOINT_ADDED, // service endpoint added
    ENDPOINT_REMOVED // service endpoint removed
  }

  private final ServiceEndpoint serviceEndpoint;
  private final Type type;

  private ServiceDiscoveryEvent(ServiceEndpoint serviceEndpoint, Type type) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public static ServiceDiscoveryEvent newEndpointAdded(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(serviceEndpoint, Type.ENDPOINT_ADDED);
  }

  public static ServiceDiscoveryEvent newEndpointRemoved(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(serviceEndpoint, Type.ENDPOINT_REMOVED);
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public Type type() {
    return this.type;
  }

  public boolean isEndpointAdded() {
    return Type.ENDPOINT_ADDED.equals(this.type);
  }

  public boolean isEndpointRemoved() {
    return Type.ENDPOINT_REMOVED.equals(this.type);
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
