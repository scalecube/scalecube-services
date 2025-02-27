package io.scalecube.services.auth;

import java.util.List;

public interface Principal {

  Principal NULL_PRINCIPAL = new Principal() {};

  default List<String> permissions() {
    return null;
  }

  default boolean hasPermission(String permission) {
    return false;
  }
}
