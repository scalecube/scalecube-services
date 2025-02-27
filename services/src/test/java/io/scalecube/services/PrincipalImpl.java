package io.scalecube.services;

import io.scalecube.services.auth.Principal;
import java.util.List;

public record PrincipalImpl(String serviceRole, List<String> permissions) implements Principal {

  @Override
  public boolean hasPermission(String permission) {
    return permissions.contains(permission);
  }
}
