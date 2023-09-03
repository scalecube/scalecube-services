package io.scalecube.services;

import io.scalecube.net.Address;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ServiceEndpoint implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String id;
  private List<Address> addresses;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;

  /**
   * Constructor for de/serialization purpose.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  @Deprecated
  public ServiceEndpoint() {}

  private ServiceEndpoint(Builder builder) {
    this.id = Objects.requireNonNull(builder.id, "ServiceEndpoint.id is required");
    this.addresses =
        Objects.requireNonNull(builder.addresses, "ServiceEndpoint.addresses is required");
    this.contentTypes = Collections.unmodifiableSet(new HashSet<>(builder.contentTypes));
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.serviceRegistrations =
        Collections.unmodifiableList(new ArrayList<>(builder.serviceRegistrations));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(ServiceEndpoint serviceEndpoint) {
    return new Builder(serviceEndpoint);
  }

  public String id() {
    return id;
  }

  public List<Address> addresses() {
    return addresses;
  }

  public Set<String> contentTypes() {
    return contentTypes;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }

  /**
   * Creates collection of service references from {@code serviceRegistrations}.
   *
   * @return {@link ServiceReference} collection
   */
  public Collection<ServiceReference> serviceReferences() {
    return serviceRegistrations.stream()
        .flatMap(sr -> sr.methods().stream().map(sm -> new ServiceReference(sm, sr, this)))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceEndpoint.class.getSimpleName() + "[", "]")
        .add("id=" + id)
        .add("addresses=" + addresses)
        .add("contentTypes=" + contentTypes)
        .add("tags=" + tags)
        .add("serviceRegistrations(" + serviceRegistrations.size() + ")")
        .toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // id
    out.writeUTF(id);

    // addresses
    out.writeInt(addresses.size());
    for (Address address : addresses) {
      out.writeUTF(address.toString());
    }

    // contentTypes
    out.writeInt(contentTypes.size());
    for (String contentType : contentTypes) {
      out.writeUTF(contentType);
    }

    // tags
    out.writeInt(tags.size());
    for (Entry<String, String> entry : tags.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeObject(entry.getValue()); // value is nullable
    }

    // serviceRegistrations
    out.writeInt(serviceRegistrations.size());
    for (ServiceRegistration registration : serviceRegistrations) {
      out.writeObject(registration);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // id
    id = in.readUTF();

    // addresses
    final int addressesSize = in.readInt();
    addresses = new ArrayList<>(addressesSize);
    for (int i = 0; i < addressesSize; i++) {
      addresses.add(Address.from(in.readUTF()));
    }

    // contentTypes
    int contentTypesSize = in.readInt();
    Set<String> contentTypes = new HashSet<>(contentTypesSize);
    for (int i = 0; i < contentTypesSize; i++) {
      contentTypes.add(in.readUTF());
    }
    this.contentTypes = Collections.unmodifiableSet(contentTypes);

    // tags
    int tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = (String) in.readObject(); // value is nullable
      tags.put(key, value);
    }
    this.tags = Collections.unmodifiableMap(tags);

    // serviceRegistrations
    int serviceRegistrationsSize = in.readInt();
    List<ServiceRegistration> serviceRegistrations = new ArrayList<>(serviceRegistrationsSize);
    for (int i = 0; i < serviceRegistrationsSize; i++) {
      serviceRegistrations.add((ServiceRegistration) in.readObject());
    }
    this.serviceRegistrations = Collections.unmodifiableList(serviceRegistrations);
  }

  public static class Builder {

    private String id;
    private List<Address> addresses = new ArrayList<>();
    private Set<String> contentTypes = Collections.emptySet();
    private Map<String, String> tags = Collections.emptyMap();
    private Collection<ServiceRegistration> serviceRegistrations = new ArrayList<>();

    private Builder() {}

    private Builder(ServiceEndpoint other) {
      this.id = other.id;
      this.addresses = new ArrayList<>(other.addresses);
      this.contentTypes = new HashSet<>(other.contentTypes);
      this.tags = new HashMap<>(other.tags);
      this.serviceRegistrations = new ArrayList<>(other.serviceRegistrations);
    }

    public Builder id(String id) {
      this.id = Objects.requireNonNull(id, "id");
      return this;
    }

    public Builder addresses(Address... addresses) {
      return addresses(Arrays.asList(addresses));
    }

    public Builder addresses(List<Address> addresses) {
      this.addresses = new ArrayList<>(Objects.requireNonNull(addresses, "addresses"));
      return this;
    }

    public Builder contentTypes(Set<String> contentTypes) {
      this.contentTypes = new HashSet<>(Objects.requireNonNull(contentTypes, "contentTypes"));
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = new HashMap<>(Objects.requireNonNull(tags, "tags"));
      return this;
    }

    /**
     * Adds {@code serviceRegistrations} to collection of {@code serviceRegistrations}.
     *
     * @param serviceRegistrations serviceRegistrations
     * @return this builder
     */
    public Builder appendServiceRegistrations(
        Collection<ServiceRegistration> serviceRegistrations) {
      this.serviceRegistrations.addAll(
          Objects.requireNonNull(serviceRegistrations, "serviceRegistrations"));
      return this;
    }

    /**
     * Setter for {@code serviceRegistrations}.
     *
     * @param serviceRegistrations serviceRegistrations
     * @return this builder
     */
    public Builder serviceRegistrations(Collection<ServiceRegistration> serviceRegistrations) {
      this.serviceRegistrations =
          new ArrayList<>(Objects.requireNonNull(serviceRegistrations, "serviceRegistrations"));
      return this;
    }

    public ServiceEndpoint build() {
      return new ServiceEndpoint(this);
    }
  }
}
