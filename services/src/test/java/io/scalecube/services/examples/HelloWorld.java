package io.scalecube.services.examples;

public class HelloWorld implements GreetingService{

  public String greeting(String name){
    return " hello to: " + name;
  }
  
}
