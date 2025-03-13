package io.scalecube.services.methods;

import java.util.Arrays;
import java.util.Objects;
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
  public ServiceRoleDefinition(String role, String... permissions) {
    this(role, Set.copyOf(Arrays.asList(permissions)));
  }

  /**
   * Constructor.
   *
   * @param role service role
   * @param permissions service permissions
   */
  public ServiceRoleDefinition(String role, Set<String> permissions) {
    this.role = role;
    this.permissions = permissions;
  }

  public String role() {
    return role;
  }

  public Set<String> permissions() {
    return permissions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final var that = (ServiceRoleDefinition) o;
    return Objects.equals(role, that.role) && Objects.equals(permissions, that.permissions);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(role);
    result = 31 * result + Objects.hashCode(permissions);
    return result;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceRoleDefinition.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
