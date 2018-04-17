package io.scalecube.services;

import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class ServiceInstance {

  private final Class<?> serviceInterface;
  final String serviceName;
  final Address address;
  final Boolean isLocal;
  final Map<String, String> tags;
  final Collection<String> methods;

  public ServiceInstance(Class<?> serviceInterface,
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
