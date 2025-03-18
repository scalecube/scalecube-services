package io.scalecube.services.auth;

import java.util.Collection;

public interface Principal {

  Principal NULL_PRINCIPAL =
      new Principal() {
        @Override
        public String toString() {
          return "NULL_PRINCIPAL";
        }
      };

  default String role() {
    return null;
  }

  default boolean hasRole(String role) {
    return false;
  }

  default Collection<String> permissions() {
    return null;
  }

  default boolean hasPermission(String permission) {
    return false;
  }
}
