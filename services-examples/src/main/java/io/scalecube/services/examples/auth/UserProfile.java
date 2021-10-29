package io.scalecube.services.examples.auth;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class UserProfile {

  private final String name;
  private final String role;

  public UserProfile(String name, String role) {
    this.name = Objects.requireNonNull(name, "UserProfile.name");
    this.role = Objects.requireNonNull(role, "UserProfile.role");
  }

  public static UserProfile fromHeaders(Map<String, String> headers) {
    return new UserProfile(headers.get("userProfile.name"), headers.get("userProfile.role"));
  }

  public String name() {
    return name;
  }

  public String role() {
    return role;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", UserProfile.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("role='" + role + "'")
        .toString();
  }
}
