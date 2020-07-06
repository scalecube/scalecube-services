package io.scalecube.services.examples.auth;

import java.util.StringJoiner;

public class UserProfile {

  private final String name;
  private final String role;

  public UserProfile(String name, String role) {
    this.name = name;
    this.role = role;
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
