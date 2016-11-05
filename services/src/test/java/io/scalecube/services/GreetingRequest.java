package io.scalecube.services;

final class GreetingRequest {

  private final String name;

  public GreetingRequest(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "GreetingRequest{name='" + name + '\'' + '}';
  }
}
