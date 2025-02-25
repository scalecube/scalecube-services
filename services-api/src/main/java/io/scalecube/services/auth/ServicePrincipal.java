package io.scalecube.services.auth;

import java.util.List;
import java.util.StringJoiner;

public record ServicePrincipal(String role, List<String> permissions) {

  @Override
  public String toString() {
    return new StringJoiner(", ", ServicePrincipal.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
