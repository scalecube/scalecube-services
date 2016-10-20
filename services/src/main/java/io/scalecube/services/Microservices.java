package io.scalecube.services;

import java.util.Collection;
import java.util.Optional;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

public class Microservices {

  private final ServiceRegistry serviceRegistry;
  private final ServiceProxytFactory serviceClientFactory;
  private final ServiceProcessor serviceProcessor;
  private final ServiceDispatcher localDispatcher;
  
  private Microservices(ICluster cluster, Optional <ServiceDiscovery> discovery) {
    this.serviceProcessor = new AnnotationServiceProcessor();
    this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
    this.serviceClientFactory = new ServiceProxytFactory(serviceRegistry,serviceProcessor);
    localDispatcher = new ServiceDispatcher(cluster, serviceRegistry);
    
    if(discovery.isPresent()){
      discovery.get().cluster(cluster);
      serviceRegistry.start(discovery.get());
    }
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
  
  public static final class Builder{
    private ICluster cluster;
    private ServiceDiscovery discovery;

    public Builder cluster(ICluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder discovery(ServiceDiscovery discovery) {
      this.discovery = discovery;
      return this;
    }

    public Microservices build() {
      return new Microservices(cluster, Optional.ofNullable(discovery));
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }

}
