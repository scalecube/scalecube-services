package io.scalecube.services;

import io.scalecube.services.auth.Principal;
import java.util.List;

public record PrincipalImpl(String serviceRole, List<String> permissions) implements Principal {

  public static final PrincipalImpl NULL_PRINCIPAL = new PrincipalImpl(null, null);

  @Override
  public boolean hasPermission(String permission) {
    return permissions.contains(permission);
  }
}
