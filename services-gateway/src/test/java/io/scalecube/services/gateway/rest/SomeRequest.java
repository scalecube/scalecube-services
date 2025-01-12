package io.scalecube.services.gateway.rest;

import java.util.StringJoiner;

public class SomeRequest {

  private String name;

  public String name() {
    return name;
  }

  public SomeRequest name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SomeRequest.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .toString();
  }
}
