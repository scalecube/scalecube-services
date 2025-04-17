package io.scalecube.services.sut;

import java.util.StringJoiner;

public final class GreetingResponse {

  private String result;
  private String sender;

  public GreetingResponse() {}

  public GreetingResponse(String result) {
    this.result = result;
    this.sender = null;
  }

  public GreetingResponse(String result, String sender) {
    this.result = result;
    this.sender = sender;
  }

  public String result() {
    return result;
  }

  public GreetingResponse result(String result) {
    this.result = result;
    return this;
  }

  public String sender() {
    return sender;
  }

  public GreetingResponse sender(String sender) {
    this.sender = sender;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GreetingResponse.class.getSimpleName() + "[", "]")
        .add("result='" + result + "'")
        .add("sender='" + sender + "'")
        .toString();
  }
}
