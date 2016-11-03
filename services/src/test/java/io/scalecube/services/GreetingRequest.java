package io.scalecube.services;

public class GreetingRequest {

  private final String name;

  public GreetingRequest(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }
}
