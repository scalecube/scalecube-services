package io.scalecube.services;

public interface IRouter {

  ServiceInstance route(String serviceName);
}
