package io.scalecube.services;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;

public class ServiceRegistration implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String namespace;
  private Map<String, String> tags;
  private Collection<ServiceMethodDefinition> methods;

  /**
   * Constructor for de/serialization purpose.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  @Deprecated
  public ServiceRegistration() {}

  /**
   * Create a new service registration.
   *
   * @param namespace the namespace to use
   * @param tags tags
   * @param methods a collection of service method definitions
   */
  public ServiceRegistration(
      String namespace, Map<String, String> tags, Collection<ServiceMethodDefinition> methods) {
    this.namespace = Objects.requireNonNull(namespace, "ServiceRegistration.namespace is required");
    this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
    this.methods = Collections.unmodifiableList(new ArrayList<>(methods));
  }

  public String namespace() {
    return namespace;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public Collection<ServiceMethodDefinition> methods() {
    return methods;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceRegistration.class.getSimpleName() + "[", "]")
        .add("namespace=" + namespace)
        .add("tags=" + tags)
        .add("methods(" + methods.size() + ")")
        .toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // namespace
    out.writeUTF(namespace);

    // tags
    out.writeInt(tags.size());
    for (Entry<String, String> entry : tags.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeObject(entry.getValue()); // value is nullable
    }

    // methods
    out.writeInt(methods.size());
    for (ServiceMethodDefinition method : methods) {
      out.writeObject(method);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // namespace
    namespace = in.readUTF();

    // tags
    int tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = (String) in.readObject(); // value is nullable
      tags.put(key, value);
    }
    this.tags = Collections.unmodifiableMap(tags);

    // methods
    int methodsSize = in.readInt();
    List<ServiceMethodDefinition> methods = new ArrayList<>(methodsSize);
    for (int i = 0; i < methodsSize; i++) {
      methods.add((ServiceMethodDefinition) in.readObject());
    }
    this.methods = Collections.unmodifiableList(methods);
  }
}
