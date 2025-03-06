package io.scalecube.services.methods;

import java.util.Set;
import java.util.StringJoiner;

public record ServiceRoleDefinition(String role, Set<String> permissions) {

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceRoleDefinition.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
