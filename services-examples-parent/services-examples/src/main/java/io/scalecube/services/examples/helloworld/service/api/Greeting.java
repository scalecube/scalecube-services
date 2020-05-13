package io.scalecube.services.examples.helloworld.service.api;

import java.io.Serializable;

public class Greeting implements Serializable {

  String message;

  public Greeting() {}

  public Greeting(String message) {
    this.message = message;
  }

  public String message() {
    return message;
  }
}
