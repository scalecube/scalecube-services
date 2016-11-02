package io.scalecube.services.examples;

public class GreetingResponse {

  private final String result;

  public GreetingResponse(String result) {
    this.result = result;
  }

  public String result() {
    return result;
  }

}
