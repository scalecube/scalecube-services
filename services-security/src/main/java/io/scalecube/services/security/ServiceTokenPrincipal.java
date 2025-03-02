package io.scalecube.services.security;

import io.scalecube.services.auth.Principal;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class ServiceTokenPrincipal implements Principal {

  private final String role;
  private final Set<String> permissions;

  public ServiceTokenPrincipal(String role, Set<String> permissions) {
    this.role = role;
    this.permissions = permissions;
  }

  @Override
  public String role() {
    return role;
  }

  @Override
  public boolean hasRole(String role) {
    return Objects.equals(this.role, role);
  }

  @Override
  public Collection<String> permissions() {
    return permissions;
  }

  @Override
  public boolean hasPermission(String permission) {
    return permissions.contains(permission);
  }
}
