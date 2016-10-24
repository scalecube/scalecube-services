package io.scalecube.services.examples;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

@Service
public interface GreetingService {

  @ServiceMethod
  String greeting(String name);
  
}
