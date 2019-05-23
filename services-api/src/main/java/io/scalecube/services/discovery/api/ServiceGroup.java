package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

public class ServiceGroup {

  public static final String GROUP_ID = "groupId";
  public static final String GROUP_SIZE = "groupSize";

  private final String groupId;
  private final Collection<ServiceEndpoint> serviceEndpoints;

  public ServiceGroup(String groupId) {
    this.groupId = Objects.requireNonNull(groupId, "groupId is required");
    this.serviceEndpoints = null;
  }

  public ServiceGroup(String groupId, Collection<ServiceEndpoint> serviceEndpoints) {
    this.groupId = Objects.requireNonNull(groupId, "groupId is required");
    this.serviceEndpoints =
        Collections.unmodifiableCollection(
            Objects.requireNonNull(serviceEndpoints, "serviceEndpoints is required"));
  }

  public String groupId() {
    return groupId;
  }

  public Optional<Collection<ServiceEndpoint>> serviceEndpoints() {
    return Optional.ofNullable(serviceEndpoints);
  }

  public int groupSize() {
    return serviceEndpoints != null ? serviceEndpoints.size() : 0;
  }

  @Override
  public String toString() {
    return "ServiceGroup{"
        + "groupId='"
        + groupId
        + '\''
        + ", serviceEndpoints="
        + serviceEndpoints
        + '}';
  }
}
