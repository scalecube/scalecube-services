package io.scalecube.gateway.websocket;

public class GreetingResponse {

  private String text;

  GreetingResponse() {
  }

  public GreetingResponse(String text) {
    this.text = text;
  }

  public String getText() {
    return text;
  }

  @Override
  public String toString() {
    return "GreetingResponse{" +
        "text='" + text + '\'' +
        '}';
  }
}
