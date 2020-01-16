package io.scalecube.services;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A Service Method Definition is a single method definition of a service inside service
 * registration.
 */
public class ServiceMethodDefinition implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String action;
  private Map<String, String> tags;
  private boolean auth;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceMethodDefinition() {}

  /**
   * Create a new Service Method Definition.
   *
   * @param action method name
   */
  public ServiceMethodDefinition(String action) {
    this(action, Collections.emptyMap(), false);
  }

  /**
   * Create a new Service Method Definition.
   *
   * @param action method name
   * @param tags tags of this method
   * @param auth is method protected by authentication
   */
  public ServiceMethodDefinition(String action, Map<String, String> tags, boolean auth) {
    this.action = Objects.requireNonNull(action);
    this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
    this.auth = auth;
  }

  public String action() {
    return action;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public boolean isAuth() {
    return auth;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceMethodDefinition.class.getSimpleName() + "[", "]")
        .add("action=" + action)
        .add("tags=" + tags)
        .add("auth=" + auth)
        .toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // action
    out.writeUTF(action);

    // tags
    out.writeInt(tags.size());
    for (Entry<String, String> entry : tags.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }

    // auth
    out.writeBoolean(auth);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // namespace
    action = in.readUTF();

    // tags
    int tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      tags.put(key, value);
    }
    this.tags = Collections.unmodifiableMap(tags);

    // auth
    this.auth = in.readBoolean();
  }
}
