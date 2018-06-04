package io.scalecube.gateway.websocket;

public class EchoMessage {

  private String text;

  public EchoMessage() {}

  public EchoMessage(String text) {
    this.text = text;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return "EchoMessage{" +
        "text='" + text + '\'' +
        '}';
  }
}
