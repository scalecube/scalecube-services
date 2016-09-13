package io.scalecube.services.examples;

/**
 * @author Anton Kharenko
 */
public class HelloResponse {

  private final String greeting;

  public HelloResponse(String greeting) {
    this.greeting = greeting;
  }

  public String getGreeting() {
    return greeting;
  }

  @Override
  public String toString() {
    return "HelloResponse{" +
        "greeting='" + greeting + '\'' +
        '}';
  }
}
