package io.scalecube.services;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.RoundRubinServiceRouter;
import io.scalecube.services.routing.Router;

public class Microservices {

  private final ServiceRegistry serviceRegistry;
  private final ServiceProxytFactory proxyFactory;
  private final ServiceProcessor serviceProcessor;
  private final ServiceDispatcher localDispatcher;

  private Microservices(ICluster cluster, Optional<ServiceDiscovery> discovery) {
    this.serviceProcessor = new AnnotationServiceProcessor();
    this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
    this.proxyFactory = new ServiceProxytFactory(serviceRegistry, serviceProcessor);

    localDispatcher = new ServiceDispatcher(cluster, serviceRegistry);

    if (discovery.isPresent()) {
      discovery.get().cluster(cluster);
      serviceRegistry.start(discovery.get());
    }
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  private <T> T createProxy(Class<T> serviceInterface, Class<? extends Router> router, 
      int timeOut,TimeUnit timeUnit) {
    
    return proxyFactory.createProxy(serviceInterface, router,timeOut,timeUnit);
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public Collection<ServiceInstance> serviceLookup(String serviceName) {
    return serviceRegistry.serviceLookup(serviceName);
  }

  public static final class Builder {
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

  public RegistrationContext registry() {
    return new RegistrationContext();
  }


  public class RegistrationContext {
    
    private Object service;

    public Object service() {
      return service;
    }

    private Router router;

    public Router router() {
      return router;
    }

    private String[] tags = {"microservice"};

    public String[] tags() {
      return tags;
    }
    
    public RegistrationContext tags(String... tags) {
      this.tags = tags;
      return this;
    }

    public RegistrationContext service(Object service) {
      this.service = service;
      return this;
    }

    public RegistrationContext router(Router router) {
      this.router = router;
      return this;
    }

    public void register() {
      serviceRegistry.registerService(this.service, tags);
    }


  }

  public ProxyContext proxy() {
    return new ProxyContext();
  }

  public class ProxyContext {

    private Class<?> api;

    public Class<?> api() {
      return api;
    }

    private Class<? extends Router> router = RoundRubinServiceRouter.class;
    private int timeOut = 10;
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    public Class<? extends Router> router() {
      return router;
    }

    public <T> ProxyContext api(Class<T> api) {
      this.api = api;
      return this;
    }

    public ProxyContext router(Class<? extends Router> router) {
      this.router = router;
      return this;
    }

    public <T> T create() {
      return (T) createProxy(this.api, router,timeOut,timeUnit);
    }

    public ProxyContext timeout(int timeOut, TimeUnit timeUnit) {
      this.timeOut = timeOut;
      this.timeUnit = timeUnit;
      return this;
    }
  }

  public ICluster cluster() {
    return this.cluster();
  }

}
