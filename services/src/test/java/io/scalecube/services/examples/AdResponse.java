package io.scalecube.services.examples;

public class AdResponse {

  private final String greeting;

  public AdResponse(String greeting) {
    this.greeting = greeting;
  }

  public String getGreeting() {
    return greeting;
  }

  @Override
  public String toString() {
    return "AdResponse{" +
        "zone Id='" + greeting + '\'' +
        '}';
  }
}
