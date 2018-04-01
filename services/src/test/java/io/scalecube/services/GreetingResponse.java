package io.scalecube.services;

final class GreetingResponse {

  private final String result;
  private final String sender;

  public GreetingResponse(String result){
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
    return "GreetingResponse{result='" + result + '\'' + '}';
  }

  public String sender() {
    return sender;
  }
}
