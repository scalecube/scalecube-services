package io.scalecube.services.examples;

public class GreetingRequest {

  String name;

  public GreetingRequest(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }
}
