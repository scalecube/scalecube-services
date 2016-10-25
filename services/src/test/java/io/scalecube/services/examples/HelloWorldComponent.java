package io.scalecube.services.examples;

public class HelloWorldComponent implements GreetingService{

  public String greeting(String name){
    return " hello to: " + name;
  }
  
}
