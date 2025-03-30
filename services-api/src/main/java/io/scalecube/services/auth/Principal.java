package io.scalecube.services.auth;

import java.util.Collection;

/**
 * Represents an authenticated entity in the system, typically service identity or end user. This
 * interface provides methods to check roles and permissions for authorization purposes.
 */
public interface Principal {

  /**
   * A constant representing a "null" principal, which signifies an unauthenticated entity. This
   * principal does not have any roles or permissions.
   */
  Principal NULL_PRINCIPAL =
      new Principal() {
        @Override
        public String toString() {
          return "NULL_PRINCIPAL";
        }
      };

  /**
   * Returns the role associated with this principal.
   *
   * @return the role name, or {@code null} if no role is assigned
   */
  default String role() {
    return null;
  }

  /**
   * Checks if this principal has the specified role.
   *
   * @param role the role to check
   * @return {@code true} if the principal has the specified role, {@code false} otherwise
   */
  default boolean hasRole(String role) {
    return false;
  }

  /**
   * Returns the collection of permissions assigned to this principal.
   *
   * @return permissions, or {@code null} if no permissions are assigned
   */
  default Collection<String> permissions() {
    return null;
  }

  /**
   * Checks if this principal has the specified permission.
   *
   * @param permission the permission to check
   * @return {@code true} if the principal has the specified permission, {@code false} otherwise
   */
  default boolean hasPermission(String permission) {
    return false;
  }
}
