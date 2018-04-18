package io.scalecube.services;

import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class ServiceReference {

  private final Class<?> serviceInterface;
  private final String serviceName;
  private final Address address;
  private final Boolean isLocal;
  private final Map<String, String> tags;
  private final Collection<String> methods;
  
  public ServiceReference(Class<?> serviceInterface,
      String serviceName,
      Collection<String> methods,
      Map<String, String> tags,
      Address address,
      Boolean isLocal) {
    
    this.serviceInterface =  serviceInterface;
    this.serviceName = serviceName;
    this.methods = methods;
    this.tags = tags;
    this.address = address;
    this.isLocal = isLocal;
  }

  public Class<?> serviceinterface() {
    return this.serviceInterface;
  }
  
  public String serviceName() {
    return this.serviceName;
  }

  public Boolean isLocal() {
    return this.isLocal;
  }

  public  Map<String, String> tags(){
    return this.tags;
  }

  public  Address address() {
    return this.address;
  }

  public Collection<String> methods() {
    return this.methods;
  }

  boolean containsMethod(String methodName) {
    return methods.contains(methodName);
  }

  void checkContainsMethod(String methodName) {
    Objects.requireNonNull(methods.contains(methodName));
  }

  public Object serviceObject() {
    // TODO Auto-generated method stub
    return null;
  }

 

}
