package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

/**
 * @author Anton Kharenko
 */
public class ServiceFabric {

    private final ICluster cluster;
    private final ServiceDispatcher serviceDispatcher;
    private final ServiceRegistry serviceRegistry;
    private final ServiceClientFactory serviceClientFactory;
    private final ServiceProcessor serviceProcessor;

    public ServiceFabric(ICluster cluster) {
        this.cluster = cluster;
        this.serviceProcessor = new AnnotationServiceProcessor();
        this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
        this.serviceDispatcher = new ServiceDispatcher(cluster, serviceRegistry);
        this.serviceClientFactory = new ServiceClientFactory(cluster, serviceRegistry, serviceProcessor);

        serviceRegistry.start();
    }

    public static ServiceFabric newInstance(ICluster cluster) {
        return new ServiceFabric(cluster);
    }

    public void registerService(Object serviceObject) {
        serviceRegistry.registerService(serviceObject);
    }

    public void unregisterService(Object serviceObject) {
        serviceRegistry.unregisterService(serviceObject);
    }

    public <T> T createServiceClient(Class<T> serviceInterface) {
        return serviceClientFactory.createClient(serviceInterface);
    }

}