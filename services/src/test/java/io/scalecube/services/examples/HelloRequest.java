package io.scalecube.services.examples;

/**
 * @author Anton Kharenko
 */
public class HelloRequest {

  private final String name;

  public HelloRequest(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "HelloRequest{" +
        "name='" + name + '\'' +
        '}';
  }
}
