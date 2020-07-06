package io.scalecube.services.examples.auth;

import java.util.StringJoiner;

public class ApiKey {

  private final String id;
  private final String permissions;

  public ApiKey(String id, String permissions) {
    this.id = id;
    this.permissions = permissions;
  }

  public String id() {
    return id;
  }

  public String permissions() {
    return permissions;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ApiKey.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("permissions='" + permissions + "'")
        .toString();
  }
}
