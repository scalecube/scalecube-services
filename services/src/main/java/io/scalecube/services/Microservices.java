package io.scalecube.services;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.RoundRubinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

public class Microservices {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);
  private static final ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private final ICluster cluster;

  private final ServiceRegistry serviceRegistry;

  private final ServiceProxytFactory proxyFactory;

  private final ServiceDispatcher localDispatcher;

  private Microservices(ICluster cluster, Optional<Object[]> services, boolean isSeed) {
    this.cluster = cluster;
    this.serviceRegistry = new ServiceRegistry(cluster, services, serviceProcessor, isSeed);
    this.proxyFactory = new ServiceProxytFactory(serviceRegistry, serviceProcessor);
    localDispatcher = new ServiceDispatcher(cluster, serviceRegistry);
  }

  public ICluster cluster() {
    return this.cluster;
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

  public static final class Builder {

    private Integer port = null;
    private Address[] seeds;
    private Optional<Object[]> services = Optional.empty();
    
    /**
     * microsrrvices instance builder.
     * @return Microservices instance.
     */
    public Microservices build() {

      ClusterConfig cfg = getClusterConfig();

      Microservices microserices = new Microservices(
          Cluster.joinAwait(cfg),
          services,
          seeds == null);

      return microserices;
    }

    private ClusterConfig getClusterConfig() {
      Map<String, String> metadata = new HashMap<String, String>();

      if (services.isPresent()) {
        metadata = Microservices.metadata(services.get());
      }

      ClusterConfig cfg;
      if (port != null && seeds != null) {
        cfg = ConfigAssist.create(port, seeds, metadata);
      } else if (seeds != null) {
        cfg = ConfigAssist.create(seeds, metadata);
      } else if (port != null) {
        cfg = ConfigAssist.create(port, metadata);
      } else {
        cfg = ConfigAssist.create(metadata);
      }
      return cfg;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder seeds(Address... seeds) {
      this.seeds = seeds;
      return this;
    }

    public Builder services(Object... services) {
      this.services = Optional.of(services);
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }


  public ProxyContext proxy() {
    return new ProxyContext();
  }

  public class ProxyContext {
    private Class<?> api;

    private Class<? extends Router> router = RoundRubinServiceRouter.class;

    public <T> T create() {
      LOGGER.debug("create service api {} router {}", this.api, router);
      return (T) createProxy(this.api, router);
    }

    public Class<?> api() {
      return api;
    }

    public <T> ProxyContext api(Class<T> api) {
      this.api = api;
      return this;
    }

    public Class<? extends Router> router() {
      return router;
    }

    public ProxyContext router(Class<? extends Router> router) {
      this.router = router;
      return this;
    }
  }

  private static Map<String, String> metadata(Object... services) {
    Map<String, String> result = new HashMap<>();

    for (Object service : services) {
      Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(service);
      for (Class<?> serviceInterface : serviceInterfaces) {
        ConcurrentMap<String, ServiceDefinition> defs = serviceProcessor.introspectServiceInterface(serviceInterface);
        defs.entrySet().stream().forEach(entry -> {
          result.put(entry.getValue().qualifier(), "service");
        });
      }
    }
    return result;
  }

}
