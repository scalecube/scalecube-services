package io.scalecube.services.examples;

public class AdRequest {

  private final String name;

  public AdRequest(String name) {
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
