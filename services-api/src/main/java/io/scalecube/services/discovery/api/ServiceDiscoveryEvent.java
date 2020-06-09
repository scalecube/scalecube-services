package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Objects;
import java.util.StringJoiner;

public class ServiceDiscoveryEvent {

  public enum Type {
    ENDPOINT_ADDED, // service endpoint added
    ENDPOINT_LEAVING, // service endpoint is leaving
    ENDPOINT_REMOVED // service endpoint removed
  }

  private final Type type;
  private final ServiceEndpoint serviceEndpoint;

  /**
   * Constructor.
   *
   * @param type type; not null
   * @param serviceEndpoint service endpoint; not null
   */
  private ServiceDiscoveryEvent(Type type, ServiceEndpoint serviceEndpoint) {
    this.type = Objects.requireNonNull(type, "ServiceDiscoveryEvent: type");
    this.serviceEndpoint =
        Objects.requireNonNull(serviceEndpoint, "ServiceDiscoveryEvent: serviceEndpoint");
  }

  public static ServiceDiscoveryEvent newEndpointAdded(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(Type.ENDPOINT_ADDED, serviceEndpoint);
  }

  public static ServiceDiscoveryEvent newEndpointLeaving(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(Type.ENDPOINT_LEAVING, serviceEndpoint);
  }

  public static ServiceDiscoveryEvent newEndpointRemoved(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(Type.ENDPOINT_REMOVED, serviceEndpoint);
  }

  public Type type() {
    return type;
  }

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public boolean isEndpointAdded() {
    return Type.ENDPOINT_ADDED == type;
  }

  public boolean isEndpointLeaving() {
    return Type.ENDPOINT_LEAVING == type;
  }

  public boolean isEndpointRemoved() {
    return Type.ENDPOINT_REMOVED == type;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceDiscoveryEvent.class.getSimpleName() + "[", "]")
        .add("type=" + type)
        .add("ServiceEndpoint.id='" + serviceEndpoint.id() + "'")
        .toString();
  }
}
