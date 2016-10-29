package io.scalecube.services;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.RoundRubinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

public class Microservices {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);
  private final static ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private final ServiceRegistry serviceRegistry;
  private final ServiceProxytFactory proxyFactory;

  private final ServiceDispatcher localDispatcher;

  private Microservices(ICluster cluster, Optional<ServiceDiscovery> discovery) {
    this.serviceRegistry = new ServiceRegistry(cluster, serviceProcessor);
    this.proxyFactory = new ServiceProxytFactory(serviceRegistry, serviceProcessor);

    localDispatcher = new ServiceDispatcher(cluster, serviceRegistry);

    if (discovery.isPresent()) {
      discovery.get().cluster(cluster);
      serviceRegistry.start(discovery.get());
    } else {
      LOGGER.warn("no service discovery was found this node services will not be discovered out-side of this process");
    }
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  private <T> T createProxy(Class<T> serviceInterface, Class<? extends Router> router) {

    return (T) proxyFactory.createProxy(serviceInterface, router);
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
    private Integer port = null;
    private Address[] seeds;
    private Object[] services;

    public Builder cluster(ICluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder discovery(ServiceDiscovery discovery) {
      this.discovery = discovery;
      return this;
    }

    public Microservices build() {
      
      ClusterConfig cfg = getClusterConfig();
      
      this.cluster = Cluster.joinAwait(cfg);

      Microservices microserices = new Microservices(cluster, Optional.ofNullable(discovery));
      for (Object service : services) {
        microserices.registry()
          .service(service)
          .register();
      }
      return microserices;
    }

    private ClusterConfig getClusterConfig() {
      Map<String, String> metadata = Microservices.metadata(services);
      ClusterConfig cfg;      
      if(port !=null && seeds!=null){
        cfg = ConfigAssist.create(port,seeds,metadata);
      }else if (seeds!=null){
        cfg = ConfigAssist.create(seeds,metadata);
      }else if(port!=null){
        cfg = ConfigAssist.create(port,metadata);
      }else{
        cfg = ConfigAssist.create(metadata);
      }
      return cfg;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder seeds(Address[] seeds) {
      this.seeds = seeds;
      return this;
    }

    public Builder services(Object... services) {
      this.services = services;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public RegistrationContext registry() {
    return new RegistrationContext();
  }


  public class RegistrationContext {

    public static final String TAG_MICROSERVICE = "microservice";

    private Object service;

    public Object service() {
      return service;
    }

    private Router router;

    public Router router() {
      return router;
    }

    private String[] tags = {TAG_MICROSERVICE};

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
      LOGGER.debug("register service {} tags {}", this.service, this.tags);
      serviceRegistry.registerService(this.service, this.tags);
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
      LOGGER.debug("create service api {} router {}", this.api, router);
      return (T) createProxy(this.api, router);
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

  public static Map<String, String> metadata(Object... services) {
    Map<String, String> result = new HashMap<>();

    for (Object service : services) {
      Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(service);
      for (Class<?> serviceInterface : serviceInterfaces) {
        ConcurrentMap<String, ServiceDefinition> defs = serviceProcessor.introspectServiceInterface(serviceInterface);
        defs.entrySet().stream().forEach(entry -> {
          result.put(entry.getValue().qualifier(), entry.getValue().method().getName());
        });
      }
    }

    return result;
  }

}
