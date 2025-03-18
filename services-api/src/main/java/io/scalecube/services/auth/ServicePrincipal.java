package io.scalecube.services.auth;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class ServicePrincipal implements Principal {

  private final String role;
  private final Set<String> permissions;

  /**
   * Constructor.
   *
   * @param role service role
   * @param permissions service permissions
   */
  public ServicePrincipal(String role, Set<String> permissions) {
    this.role = role;
    this.permissions = Set.copyOf(permissions);
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
