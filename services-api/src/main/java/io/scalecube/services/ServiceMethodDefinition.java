package io.scalecube.services;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * A Service Method Definition is a single method definition of a service inside service
 * registration.
 */
public class ServiceMethodDefinition implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String action;
  private Map<String, String> tags;
  private String restMethod;
  private boolean isSecured;
  private List<String> allowedRoles;

  @Deprecated
  public ServiceMethodDefinition() {}

  private ServiceMethodDefinition(Builder builder) {
    this.action = builder.action;
    this.tags = Collections.unmodifiableMap(builder.tags);
    this.restMethod = builder.restMethod;
    this.isSecured = builder.isSecured;
    this.allowedRoles = Collections.unmodifiableList(builder.allowedRoles);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ServiceMethodDefinition fromAction(String action) {
    return new ServiceMethodDefinition.Builder().action(action).build();
  }

  public static ServiceMethodDefinition fromMethod(Method method) {
    return new Builder()
        .action(Reflect.methodName(method))
        .tags(Reflect.serviceMethodTags(method))
        .restMethod(Reflect.restMethod(method))
        .isSecured(Reflect.isSecured(method))
        .allowedRoles(Reflect.allowedRoles(method))
        .build();
  }

  public String action() {
    return action;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public String restMethod() {
    return restMethod;
  }

  public boolean isSecured() {
    return isSecured;
  }

  public List<String> allowedRoles() {
    return allowedRoles;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceMethodDefinition.class.getSimpleName() + "[", "]")
        .add("action='" + action + "'")
        .add("tags=" + tags)
        .add("restMethod='" + restMethod + "'")
        .add("isSecured=" + isSecured)
        .add("allowedRoles=" + allowedRoles)
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
      out.writeObject(entry.getValue());
    }

    // rest method
    out.writeUTF(restMethod != null ? restMethod : "");

    // auth
    out.writeBoolean(isSecured);

    // roles
    out.writeInt(allowedRoles.size());
    for (String role : allowedRoles) {
      out.writeUTF(role);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // namespace
    action = in.readUTF();

    // tags
    final var tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = (String) in.readObject();
      tags.put(key, value);
    }
    this.tags = Collections.unmodifiableMap(tags);

    // rest method
    final var restMethod = in.readUTF();
    this.restMethod = !restMethod.isEmpty() ? restMethod : null;

    // auth
    this.isSecured = in.readBoolean();

    // roles
    final var allowedRolesSize = in.readInt();
    List<String> allowedRoles = new ArrayList<>(allowedRolesSize);
    for (int i = 0; i < allowedRolesSize; i++) {
      allowedRoles.add(in.readUTF());
    }
    this.allowedRoles = Collections.unmodifiableList(allowedRoles);
  }

  public static class Builder {

    private String action;
    private Map<String, String> tags = new HashMap<>();
    private String restMethod;
    private boolean isSecured;
    private List<String> allowedRoles = new ArrayList<>();

    private Builder() {}

    public Builder action(String action) {
      this.action = action;
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder restMethod(String restMethod) {
      this.restMethod = restMethod;
      return this;
    }

    public Builder isSecured(boolean secured) {
      this.isSecured = secured;
      return this;
    }

    public Builder allowedRoles(List<String> allowedRoles) {
      this.allowedRoles = allowedRoles;
      return this;
    }

    public ServiceMethodDefinition build() {
      return new ServiceMethodDefinition(this);
    }
  }
}
