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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    GreetingResponse that = (GreetingResponse) obj;

    return text != null ? text.equals(that.text) : that.text == null;
  }

  @Override
  public int hashCode() {
    return text != null ? text.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "GreetingResponse{" + "text='" + text + '\'' + '}';
  }
}
