package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

public class Microservices {

  private final ICluster cluster;
  private final ServiceDispatcher serviceDispatcher;
  private final ServiceRegistry serviceRegistry;
  private final ServiceClientFactory serviceClientFactory;
  private final ServiceProcessor serviceProcessor;

  public Microservices(ICluster cluster) {
    this.cluster = cluster;
    this.serviceProcessor = new AnnotationServiceProcessor();
    this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
    this.serviceDispatcher = new ServiceDispatcher(cluster, serviceRegistry);
    this.serviceClientFactory = new ServiceClientFactory(cluster, serviceRegistry, serviceProcessor);

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
    return serviceClientFactory.createClient(serviceInterface);
  }

}
