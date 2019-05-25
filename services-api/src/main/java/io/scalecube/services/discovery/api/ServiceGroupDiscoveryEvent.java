package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class ServiceGroupDiscoveryEvent {

  public enum Type {
    ENDPOINT_ADDED_TO_GROUP, // service endpoint added to the group
    GROUP_ADDED, // service endpoint group added
    ENDPOINT_REMOVED_FROM_GROUP, // service endpoint added from the group
    GROUP_REMOVED // service endpoint group removed
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

  public static ServiceGroupDiscoveryEvent newGroupAdded(
      String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(Type.GROUP_ADDED, groupId, null, serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent newGroupRemoved(String groupId) {
    return new ServiceGroupDiscoveryEvent(
        Type.GROUP_REMOVED, groupId, null, Collections.emptyList());
  }

  public static ServiceGroupDiscoveryEvent newEndpointAddedToGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(
        Type.ENDPOINT_ADDED_TO_GROUP, groupId, serviceEndpoint, serviceEndpoints);
  }

  public static ServiceGroupDiscoveryEvent newEndpointRemovedFromGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceGroupDiscoveryEvent(
        Type.ENDPOINT_REMOVED_FROM_GROUP, groupId, serviceEndpoint, serviceEndpoints);
  }

  public boolean isGroupAdded() {
    return Type.GROUP_ADDED.equals(this.type);
  }

  public boolean isGroupRemoved() {
    return Type.GROUP_REMOVED.equals(this.type);
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
