package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The ScaleCube-Services module enables to provision and consuming microservices in a cluster. ScaleCube-Services
 * provides Reactive application development platform for building distributed applications Using microservices and fast
 * data on a message-driven runtime that scales transparently on multi-core, multi-process and/or multi-machines Most
 * microservices frameworks focus on making it easy to build individual microservices. ScaleCube allows developers to
 * run a whole system of microservices from a single command. removing most of the boilerplate code, ScaleCube-Services
 * focuses development on the essence of the service and makes it easy to create explicit and typed protocols that
 * compose. True isolation is achieved through shared-nothing design. This means the services in ScaleCube are
 * autonomous, loosely coupled and mobile (location transparent)—necessary requirements for resilence and elasticity
 * 
 * <p>ScaleCube services requires developers only to two simple Annotations declaring a Service but not regards how you
 * build the service component itself. the Service component is simply java class that implements the service Interface
 * and ScaleCube take care for the rest of the magic. it derived and influenced by Actor model and reactive and
 * streaming patters but does not force application developers to it.
 * 
 * <p>ScaleCube-Services is not yet-anther RPC system in the sense its is cluster aware to provide:
 * <li>location transparency and discovery of service instances.</li>
 * <li>fault tolerance using gossip and failure detection.</li>
 * <li>share nothing - fully distributed and decentralized architecture.</li>
 * <li>Provides fluent, java 8 lambda apis.</li>
 * <li>Embeddable and lightweight.</li>
 * <li>utilizes completable futures but primitives and messages can be used as well completable futures gives the
 * advantage of composing and chaining service calls and service results.</li>
 * <li>low latency</li>
 * <li>supports routing extensible strategies when selecting service end-points</li>
 * 
 * </p>
 * <b>basic usage example:</b>
 * 
 * <pre>
 * 
 * <b><font color="green">//Define a service interface and implement it.</font></b>
 * {@code
 *    <b>{@literal @}Service</b>
 *    <b><font color="9b0d9b">public interface</font></b> GreetingService {  
 *
 *         <b>{@literal @}ServiceMethod</b>
 *         CompletableFuture<String> asyncGreeting(String string);
 *     }
 *    
 *     <b><font color="9b0d9b">public class</font></b> GreetingServiceImpl implements GreetingService {
 *
 *       {@literal @}Override
 *       <b><font color="9b0d9b">public</font></b> CompletableFuture<String> asyncGreeting(String name) {
 *         <b><font color="9b0d9b">return</font></b> CompletableFuture.completedFuture(" hello to: " + name);
 *       }
 *     }
 *     <b><font color="green">//Build a microservices cluster instance.</font></b>
 *     Microservices microservices = Microservices.builder()
 *       <b><font color="green">//Introduce GreetingServiceImpl pojo as a micro-service.</font></b>
 *         .services(<b><font color="9b0d9b">new</font></b> GreetingServiceImpl())
 *         .build();
 * 
 *     <b><font color="green">//Create microservice proxy to GreetingService.class interface.</font></b>
 *     GreetingService service = microservices.proxy()
 *         .api(GreetingService.class)
 *         .create();
 * 
 *     <b><font color="green">//Invoke the greeting service async.</font></b>
 *     CompletableFuture<String> future = service.asyncGreeting("joe");
 * 
 *     <b><font color="green">//handle completable success or error.</font></b>
 *     future.whenComplete((result, ex) -> {
 *      if (ex == <b><font color="9b0d9b">null</font></b>) {
 *        // print the greeting.
 *         System.<b><font color="9b0d9b">out</font></b>.println(result);
 *       } else {
 *         // print the greeting.
 *         System.<b><font color="9b0d9b">out</font></b>.println(ex);
 *       }
 *     });
 * }
 * </pre>
 */

public class Microservices {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);
  private static final ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private final Cluster cluster;

  private final ServiceRegistry serviceRegistry;

  private final ServiceProxyFactory proxyFactory;

  private final ServiceDispatcherFactory dispatcherFactory;

  private final ServiceCommunicator sender;

  private Microservices(Cluster cluster, ServiceCommunicator sender, ServicesConfig services) {
    this.cluster = cluster;
    this.sender = sender;
    this.serviceRegistry = new ServiceRegistryImpl(cluster, sender, services, serviceProcessor);

    this.proxyFactory = new ServiceProxyFactory(this);
    this.dispatcherFactory = new ServiceDispatcherFactory(serviceRegistry);

    new ServiceDispatcher(cluster, serviceRegistry);
    this.sender.listen()
        .filter(message -> message.header(ServiceHeaders.SERVICE_RESPONSE) != null)
        .subscribe(message -> ServiceResponse.handleReply(message));
  }

  public Cluster cluster() {
    return this.cluster;
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  private <T> T createProxy(Class<T> serviceInterface, Class<? extends Router> router, Duration timeout) {
    return proxyFactory.createProxy(serviceInterface, router, timeout);
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public static final class Builder {

    private ServicesConfig servicesConfig = ServicesConfig.empty();

    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();

    private TransportConfig transportConfig;

    private boolean reuseClusterTransport;

    /**
     * microsrrvices instance builder.
     * 
     * @return Microservices instance.
     */
    public Microservices build() {

      ClusterConfig cfg = getClusterConfig(servicesConfig);

      // if transport config is not specifically set use same config as cluster.
      if (transportConfig == null) {
        transportConfig = cfg.getTransportConfig();
      }

      Cluster cluster = Cluster.joinAwait(cfg);
      ServiceCommunicator sender = new ClusterServiceCommunicator(cluster);

      if (!this.reuseClusterTransport) {
        // create cluster and transport with given config.
        sender = new TransportServiceCommunicator(Transport.bindAwait(transportConfig));
      }

      return ServiceInjector.builder(new Microservices(cluster, sender, servicesConfig)).inject();
    }

    private ClusterConfig getClusterConfig(ServicesConfig servicesConfig) {
      if (servicesConfig != null && !servicesConfig.services().isEmpty()) {
        Map<String, String> metadata = new HashMap<>();
        metadata.putAll(clusterConfig.metadata());
        metadata.putAll(Microservices.metadata(servicesConfig));
        clusterConfig.metadata(metadata);
      }

      return clusterConfig.build();
    }

    public Builder reuseClusterTransport(boolean reuse) {
      this.reuseClusterTransport = reuse;
      return this;
    }

    public Builder port(int port) {
      this.clusterConfig.port(port);
      return this;
    }

    public Builder seeds(Address... seeds) {
      this.clusterConfig.seedMembers(seeds);
      return this;
    }

    public Builder clusterConfig(ClusterConfig.Builder clusterConfig) {
      this.clusterConfig = clusterConfig;
      return this;
    }

    /**
     * Services list to be registered.
     * 
     * @param services list of instances decorated with @Service
     * @return builder.
     */
    public Builder services(Object... services) {
      checkNotNull(services);

      this.servicesConfig = ServicesConfig.builder(this)
          .services(services)
          .create();

      return this;
    }

    public ServicesConfig.Builder services() {
      return ServicesConfig.builder(this);
    }

    /**
     * Services list to be registered.
     * 
     * @param servicesConfig list of instances decorated with.
     * @return builder.
     */
    public Builder services(ServicesConfig servicesConfig) {
      checkNotNull(servicesConfig);
      this.servicesConfig = servicesConfig;
      return this;
    }

    public Builder serviceTransport(TransportConfig transportConfig) {
      this.transportConfig = transportConfig;
      return this;
    }

  }

  public static Builder builder() {
    return new Builder();
  }


  public class DispatcherContext {
    private Duration timeout = Duration.ofSeconds(30);

    private Class<? extends Router> router = RoundRobinServiceRouter.class;

    public ServiceCall create() {
      LOGGER.debug("create service api {} router {}", router);
      return dispatcherFactory.createDispatcher(this.router, this.timeout);
    }

    public DispatcherContext timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public DispatcherContext router(Class<? extends Router> routerType) {
      this.router = routerType;
      return this;
    }

    public Class<? extends Router> router() {
      return this.router;
    }
  }

  public DispatcherContext dispatcher() {
    return new DispatcherContext();
  }

  public ProxyContext proxy() {
    return new ProxyContext();
  }

  public class ProxyContext {
    private Class<?> api;

    private Class<? extends Router> router = RoundRobinServiceRouter.class;

    private Duration timeout = Duration.ofSeconds(3);

    @SuppressWarnings("unchecked")
    public <T> T create() {
      LOGGER.debug("create service api {} router {}", this.api, router);
      return (T) createProxy(this.api, this.router, this.timeout);
    }

    public ProxyContext timeout(Duration duration) {
      this.timeout = duration;
      return this;
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

  private static Map<String, String> metadata(ServicesConfig config) {
    Map<String, String> servicesTags = new HashMap<>();

    config.getServiceConfigs().stream().forEach(serviceConfig -> {
      serviceConfig.serviceNames().stream().forEach(name -> {
        servicesTags.put(new ServiceInfo(name, serviceConfig.getTags()).toMetadata(), "service");
      });
    });

    return servicesTags;
  }

  public ServiceCommunicator sender() {
    return sender;
  }

  public CompletableFuture<Void> shutdown() {
    return this.cluster.shutdown();
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }
}
