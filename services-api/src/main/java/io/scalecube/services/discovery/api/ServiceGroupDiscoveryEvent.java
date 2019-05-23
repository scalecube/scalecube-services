package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class ServiceGroupDiscoveryEvent {

  public enum Type {
    REGISTERED, // service endpoint group added
    UNREGISTERED // service endpoint group removed
  }

  private final Type type;
  private final String groupId;
  private final Collection<ServiceEndpoint> serviceEndpoints;

  private ServiceGroupDiscoveryEvent(
      Type type, String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    this.type = type;
    this.groupId = groupId;
    this.serviceEndpoints = Collections.unmodifiableCollection(serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent registered(
      String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(Type.REGISTERED, groupId, serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent unregistered(String groupId) {
    return new ServiceGroupDiscoveryEvent(Type.UNREGISTERED, groupId, Collections.emptyList());
  }

  public boolean isRegistered() {
    return Type.REGISTERED.equals(this.type);
  }

  public boolean isUnregistered() {
    return Type.UNREGISTERED.equals(this.type);
  }

  public String groupId() {
    return groupId;
  }

  public int groupSize() {
    return serviceEndpoints.size();
  }

  public Collection<ServiceEndpoint> serviceEndpoints() {
    return serviceEndpoints;
  }

  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return "ServiceGroupDiscoveryEvent{"
        + "type="
        + type
        + ", groupId='"
        + groupId
        + '\''
        + ", serviceEndpoints="
        + serviceEndpoints.stream() //
            .map(ServiceEndpoint::id)
            .collect(Collectors.joining(","))
        + '}';
  }
}
