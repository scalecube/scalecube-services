package io.scalecube.gateway.websocket;

public class GreetingRequest {

  private String text;

  public GreetingRequest() {}

  public GreetingRequest(String text) {
    this.text = text;
  }

  public String getText() {
    return text;
  }

  @Override
  public String toString() {
    return "GreetingRequest{" + "text='" + text + '\'' + '}';
  }
}
