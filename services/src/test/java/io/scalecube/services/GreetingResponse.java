package io.scalecube.services;

final class GreetingResponse {

  private final String result;

  public GreetingResponse(String result) {
    this.result = result;
  }

  public String getResult() {
    return result;
  }

  @Override
  public String toString() {
    return "GreetingResponse{result='" + result + '\'' + '}';
  }
}
