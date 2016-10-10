package io.scalecube.services;

import java.util.Collection;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

public class Microservices {

  private final ServiceRegistry serviceRegistry;
  private final ServiceProxytFactory serviceClientFactory;
  private final ServiceProcessor serviceProcessor;
  private final IRouter router;
  private final ServiceDispatcher localDispatcher;
  
  public Microservices(ICluster cluster) {
    this.serviceProcessor = new AnnotationServiceProcessor();
    this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
    this.router = new RandomServiceRouter(serviceRegistry);
    this.serviceClientFactory = new ServiceProxytFactory(router, serviceProcessor);
    localDispatcher = new ServiceDispatcher(cluster, serviceRegistry);
    serviceRegistry.start();
  }

  public static Microservices newInstance(ICluster cluster) {
    return new Microservices(cluster);
  }

  public void registerService(Object serviceObject) {
    serviceRegistry.registerService(serviceObject);
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  public <T> T createProxy(Class<T> serviceInterface) {
    return serviceClientFactory.createProxy(serviceInterface);
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public Collection<ServiceInstance> serviceLookup(String serviceName) {
    return serviceRegistry.serviceLookup(serviceName);
  }

}
