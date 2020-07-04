package io.scalecube.services.examples.auth;

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
}
