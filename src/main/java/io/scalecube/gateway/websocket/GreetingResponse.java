package io.scalecube.gateway.websocket;

public class GreetingResponse {

  private String text;

  public GreetingResponse() {}

  public GreetingResponse(String text) {
    this.text = text;
  }

  public String getText() {
    return text;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    GreetingResponse that = (GreetingResponse) other;

    return text != null ? text.equals(that.text) : that.text == null;
  }

  @Override
  public int hashCode() {
    return text != null ? text.hashCode() : 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GreetingResponse{");
    sb.append("text='").append(text).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
