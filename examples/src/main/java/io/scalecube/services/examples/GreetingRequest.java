package io.scalecube.services.examples;

public class GreetingRequest {

  private final String name;

  public GreetingRequest(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }
}
