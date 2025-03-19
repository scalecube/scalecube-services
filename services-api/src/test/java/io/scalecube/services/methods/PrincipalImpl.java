package io.scalecube.services.methods;

import io.scalecube.services.auth.Principal;
import java.util.List;
import java.util.Objects;

public record PrincipalImpl(String role, List<String> permissions) implements Principal {

  @Override
  public boolean hasRole(String role) {
    return Objects.equals(this.role, role);
  }

  @Override
  public boolean hasPermission(String permission) {
    return permissions != null && permissions.contains(permission);
  }
}
