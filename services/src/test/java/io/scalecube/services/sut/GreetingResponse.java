package io.scalecube.services.sut;

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

  public String getResult() {
    return result;
  }

  @Override
  public String toString() {
    return "GreetingResponse{" + "result='" + result + '\'' + ", sender='" + sender + '\'' + '}';
  }

  public String sender() {
    return sender;
  }
}
