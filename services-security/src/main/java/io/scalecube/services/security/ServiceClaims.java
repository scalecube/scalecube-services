package io.scalecube.services.security;

import java.util.StringJoiner;

public final class ServiceClaims {

  private final String permissions;

  public ServiceClaims(String permissions) {
    this.permissions = permissions;
  }

  public String permissions() {
    return permissions;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceClaims.class.getSimpleName() + "[", "]")
        .add("permissions='" + permissions + "'")
        .toString();
  }
}
