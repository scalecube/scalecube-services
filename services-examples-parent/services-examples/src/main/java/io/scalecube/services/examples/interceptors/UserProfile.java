package io.scalecube.services.examples.interceptors;

public final class UserProfile {

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
