package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

public class ServiceDiscoveryEvent {

  public enum Type {
    ENDPOINT_ADDED, // service endpoint added
    ENDPOINT_REMOVED, // service endpoint removed
    ENDPOINT_ADDED_TO_GROUP, // service endpoint added to the group
    GROUP_ADDED, // service endpoint group added
    ENDPOINT_REMOVED_FROM_GROUP, // service endpoint added from the group
    GROUP_REMOVED // service endpoint group removed
  }

  private final Type type;
  private final ServiceEndpoint serviceEndpoint;
  private final String groupId;
  private final Collection<ServiceEndpoint> serviceEndpoints;

  /**
   * Constructor.
   *
   * @param type type
   * @param serviceEndpoint service endpoint
   */
  private ServiceDiscoveryEvent(Type type, ServiceEndpoint serviceEndpoint) {
    this(type, serviceEndpoint, null, Collections.emptyList());
  }

  /**
   * Constructor.
   *
   * @param type type
   * @param serviceEndpoint service endpoint
   * @param groupId group id
   * @param serviceEndpoints service endpoints
   */
  private ServiceDiscoveryEvent(
      Type type,
      ServiceEndpoint serviceEndpoint,
      String groupId,
      Collection<ServiceEndpoint> serviceEndpoints) {
    this.type = type;
    this.serviceEndpoint = serviceEndpoint;
    this.groupId = groupId;
    this.serviceEndpoints = serviceEndpoints;
  }

  public static ServiceDiscoveryEvent newEndpointAdded(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(Type.ENDPOINT_ADDED, serviceEndpoint);
  }

  public static ServiceDiscoveryEvent newEndpointRemoved(ServiceEndpoint serviceEndpoint) {
    return new ServiceDiscoveryEvent(Type.ENDPOINT_REMOVED, serviceEndpoint);
  }

  public static ServiceDiscoveryEvent newGroupAdded(
      String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceDiscoveryEvent(Type.GROUP_ADDED, null, groupId, serviceEndpoints);
  }

  public static ServiceDiscoveryEvent newGroupRemoved(String groupId) {
    return new ServiceDiscoveryEvent(Type.GROUP_REMOVED, null, groupId, Collections.emptyList());
  }

  public static ServiceDiscoveryEvent newEndpointAddedToGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceDiscoveryEvent(
        Type.ENDPOINT_ADDED_TO_GROUP, serviceEndpoint, groupId, serviceEndpoints);
  }

  public static ServiceDiscoveryEvent newEndpointRemovedFromGroup(
      String groupId,
      ServiceEndpoint serviceEndpoint,
      Collection<ServiceEndpoint> serviceEndpoints) {
    return new ServiceDiscoveryEvent(
        Type.ENDPOINT_REMOVED_FROM_GROUP, serviceEndpoint, groupId, serviceEndpoints);
  }

  public Type type() {
    return type;
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

  public boolean isEndpointAdded() {
    return Type.ENDPOINT_ADDED == type;
  }

  public boolean isEndpointRemoved() {
    return Type.ENDPOINT_REMOVED == type;
  }

  public boolean isGroupAdded() {
    return Type.GROUP_ADDED == type;
  }

  public boolean isGroupRemoved() {
    return Type.GROUP_REMOVED == type;
  }

  public boolean isEndpointAddedToTheGroup() {
    return Type.ENDPOINT_ADDED_TO_GROUP == type;
  }

  public boolean isEndpointRemovedFromTheGroup() {
    return Type.ENDPOINT_REMOVED_FROM_GROUP == type;
  }

  @Override
  public String toString() {
    return "ServiceDiscoveryEvent{"
        + "type="
        + type
        + ", groupId='"
        + groupId
        + '\''
        + ", serviceEndpoint="
        + Optional.ofNullable(serviceEndpoint) //
            .map(ServiceEndpoint::id)
            .orElse(null)
        + ", serviceEndpoints="
        + serviceEndpoints.stream() //
            .map(ServiceEndpoint::id)
            .collect(Collectors.joining(",", "[", "]"))
        + '}';
  }
}
