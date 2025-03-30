package io.scalecube.services.methods;

import java.util.Collection;
import java.util.Set;
import java.util.StringJoiner;

public class ServiceRoleDefinition {

  private final String role;
  private final Set<String> permissions;

  /**
   * Constructor.
   *
   * @param role service role
   * @param permissions service permissions
   */
  public ServiceRoleDefinition(String role, Collection<String> permissions) {
    this.role = role;
    this.permissions = permissions != null ? Set.copyOf(permissions) : null;
  }

  public String role() {
    return role;
  }

  public Collection<String> permissions() {
    return permissions;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceRoleDefinition.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
