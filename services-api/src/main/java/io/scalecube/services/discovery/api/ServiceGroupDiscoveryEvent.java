package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class ServiceGroupDiscoveryEvent {

  public enum Type {
    ENDPOINT_ADDED_TO_GROUP, // service endpoint added to the group
    REGISTERED, // service endpoint group added
    ENDPOINT_REMOVED_FROM_GROUP, // service endpoint added from the group
    UNREGISTERED // service endpoint group removed
  }

  private final Type type;
  private final String groupId;
  private final ServiceEndpoint serviceEndpoint;
  private final Collection<ServiceEndpoint> serviceEndpoints;

  private ServiceGroupDiscoveryEvent(
      Type type,
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    this.type = type;
    this.groupId = groupId;
    this.serviceEndpoint = serviceEndpoint;
    this.serviceEndpoints = Collections.unmodifiableCollection(serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent groupRegistered(
      String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(Type.REGISTERED, groupId, null, serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent groupUnregistered(String groupId) {
    return new ServiceGroupDiscoveryEvent(
        Type.UNREGISTERED, groupId, null, Collections.emptyList());
  }

  public static ServiceGroupDiscoveryEvent endpointAddedToGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(
        Type.ENDPOINT_ADDED_TO_GROUP, groupId, serviceEndpoint, serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent endpointRemovedFromGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(
        Type.ENDPOINT_REMOVED_FROM_GROUP, groupId, serviceEndpoint, serviceEndpoints);
  }

  public boolean isGroupRegistered() {
    return Type.REGISTERED.equals(this.type);
  }

  public boolean isGroupUnregistered() {
    return Type.UNREGISTERED.equals(this.type);
  }

  public boolean isEndpointAddedToTheGroup() {
    return Type.ENDPOINT_ADDED_TO_GROUP.equals(this.type);
  }

  public boolean isEndpointRemovedFromTheGroup() {
    return Type.ENDPOINT_REMOVED_FROM_GROUP.equals(this.type);
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

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
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
        + ", serviceEndpoint="
        + serviceEndpoint.id()
        + ", serviceEndpoints="
        + serviceEndpoints.stream() //
            .map(ServiceEndpoint::id)
            .collect(Collectors.joining(",", "[", "]"))
        + '}';
  }
}
