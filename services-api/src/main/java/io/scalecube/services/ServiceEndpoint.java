package io.scalecube.services;

import io.scalecube.net.Address;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServiceEndpoint {

  private String id;
  private Address address;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;
  private ServiceGroup serviceGroup;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceEndpoint() {

  }

  private ServiceEndpoint(Builder builder) {
    this.id = builder.id;
    this.address = builder.address;
    this.contentTypes = Collections.unmodifiableSet(new HashSet<>(builder.contentTypes));
    this.tags = new HashMap<>(builder.tags);
    this.serviceRegistrations =
        Collections.unmodifiableCollection(new ArrayList<>(builder.serviceRegistrations));
    this.serviceGroup = builder.serviceGroup;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return id;
  }

  public Address address() {
    return address;
  }

  public Set<String> contentTypes() {
    return contentTypes;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public ServiceGroup serviceGroup() {
    return serviceGroup;
  }

  /**
   * Return collection of service registratrions.
   *
   * @return collection of {@link ServiceRegistration}
   */
  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }

  /**
   * Creates collection of service references from this service endpoint.
   *
   * @return collection of {@link ServiceReference}
   */
  public Collection<ServiceReference> serviceReferences() {
    return serviceRegistrations.stream()
        .flatMap(sr -> sr.methods().stream().map(sm -> new ServiceReference(sm, sr, this)))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "ServiceEndpoint{"
        + "id='"
        + id
        + '\''
        + ", address='"
        + address
        + '\''
        + ", tags="
        + tags
        + ", serviceRegistrations="
        + serviceRegistrations
        + '}';
  }

  public static class Builder {

    private String id;
    private Address address;
    private Set<String> contentTypes = Collections.emptySet();
    private Map<String, String> tags = Collections.emptyMap();
    private Collection<ServiceRegistration> serviceRegistrations = new ArrayList<>();
    private ServiceGroup serviceGroup;

    private Builder() {

    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Builder contentTypes(Set<String> contentTypes) {
      this.contentTypes = contentTypes;
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder appendServiceRegistrations(
        Collection<ServiceRegistration> serviceRegistrations) {
      this.serviceRegistrations.addAll(serviceRegistrations);
      return this;
    }

    public Builder serviceRegistrations(Collection<ServiceRegistration> serviceRegistrations) {
      this.serviceRegistrations = serviceRegistrations;
      return this;
    }

    public Builder serviceGroup(String groupId, int groupSize) {
      this.serviceGroup = new ServiceGroup(groupId, groupSize);
      return this;
    }

    public ServiceEndpoint build() {
      return new ServiceEndpoint(this);
    }
  }
}
