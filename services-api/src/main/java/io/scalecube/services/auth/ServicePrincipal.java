package io.scalecube.services.auth;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Service principal implementation of {@link Principal}. Provides role-based access control by
 * allowing checks against assigned roles and permissions.
 */
public class ServicePrincipal implements Principal {

  private final String role;
  private final Set<String> permissions;

  /**
   * Constructor.
   *
   * @param role service role
   * @param permissions service permissions
   */
  public ServicePrincipal(String role, Collection<String> permissions) {
    this.role = role;
    this.permissions = permissions != null ? Set.copyOf(permissions) : null;
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
    return permissions != null && permissions.contains(permission);
  }
}
