package io.scalecube.services.auth;

import java.util.List;
import java.util.StringJoiner;

public record Principal(String role, List<String> permissions) {

  public static final Principal NULL_PRINCIPAL = new Principal(null, null);

  @Override
  public String toString() {
    return new StringJoiner(", ", Principal.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
