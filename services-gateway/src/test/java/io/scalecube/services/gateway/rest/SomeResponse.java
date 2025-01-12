package io.scalecube.services.gateway.rest;

import java.util.StringJoiner;

public class SomeResponse {

  private String name;

  public String name() {
    return name;
  }

  public SomeResponse name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SomeResponse.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .toString();
  }
}
