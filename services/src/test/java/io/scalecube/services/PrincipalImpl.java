package io.scalecube.services;

import io.scalecube.services.auth.Principal;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public record PrincipalImpl(String role, List<String> permissions) implements Principal {

  @Override
  public boolean hasRole(String role) {
    return Objects.equals(this.role, role);
  }

  @Override
  public boolean hasPermission(String permission) {
    return permissions != null && permissions.contains(permission);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PrincipalImpl.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
