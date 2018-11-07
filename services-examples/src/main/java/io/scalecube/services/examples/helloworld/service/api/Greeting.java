package io.scalecube.services.examples.helloworld.service.api;

public class Greeting {

  String message;

  public Greeting() {}

  public Greeting(String message) {
    this.message = message;
  }

  public String message() {
    return message;
  }
}
