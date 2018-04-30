package io.scalecube.services;

public class GreetingNoConstructor {
  private String value;

  public GreetingNoConstructor(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "GreetingNoConstructor{" +
        "value='" + value + '\'' +
        '}';
  }
}
