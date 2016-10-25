package io.scalecube.services.examples;

public class GreetingResponse {

  String greeting;
  
  public GreetingResponse(String greeting){
    this.greeting = greeting;
  }
  public String greeting(){
    return greeting;
  }
  
}
