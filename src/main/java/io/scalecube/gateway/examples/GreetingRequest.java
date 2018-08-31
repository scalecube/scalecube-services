package io.scalecube.gateway.examples;

public class GreetingRequest {

  private String text;

  public GreetingRequest() {}

  public String getText() {
    return text;
  }

  public GreetingRequest setText(String text) {
    this.text = text;
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GreetingRequest{");
    sb.append("text='").append(text).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
