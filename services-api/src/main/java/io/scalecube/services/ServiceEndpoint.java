package io.scalecube.services;

import io.scalecube.net.Address;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
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
  private Address address;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private Collection<ServiceRegistration> serviceRegistrations;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceEndpoint() {}

  private ServiceEndpoint(Builder builder) {
    this.id = Objects.requireNonNull(builder.id);
    this.address = builder.address;
    this.contentTypes = Collections.unmodifiableSet(new HashSet<>(builder.contentTypes));
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.serviceRegistrations =
        Collections.unmodifiableList(new ArrayList<>(builder.serviceRegistrations));
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
        .add("address=" + address)
        .add("contentTypes=" + contentTypes)
        .add("tags=" + tags)
        .add("serviceRegistrations(" + serviceRegistrations.size() + ")")
        .toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // id
    out.writeUTF(id);

    // address
    out.writeUTF(address.toString());

    // contentTypes
    out.writeInt(contentTypes.size());
    for (String contentType : contentTypes) {
      out.writeUTF(contentType);
    }

    // tags
    out.writeInt(tags.size());
    for (Entry<String, String> entry : tags.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
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

    // address
    address = Address.from(in.readUTF());

    // contentTypes
    int capacitySize = in.readInt();
    Set<String> contentTypes = new HashSet<>(capacitySize);
    for (int i = 0; i < capacitySize; i++) {
      contentTypes.add(in.readUTF());
    }
    this.contentTypes = Collections.unmodifiableSet(contentTypes);

    // tags
    int tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
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
    private Address address;
    private Set<String> contentTypes = Collections.emptySet();
    private Map<String, String> tags = Collections.emptyMap();
    private Collection<ServiceRegistration> serviceRegistrations = new ArrayList<>();

    private Builder() {}

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

    public ServiceEndpoint build() {
      return new ServiceEndpoint(this);
    }
  }
}
