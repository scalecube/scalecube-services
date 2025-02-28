package io.scalecube.services.auth;

import java.util.Collection;

public interface Principal {

  Principal NULL_PRINCIPAL = new Principal() {};

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
