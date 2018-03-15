package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.codahale.metrics.MetricRegistry;

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
 * autonomous, loosely coupled and mobile (location transparent)—necessary requirements for resilance and elasticity
 * ScaleCube services requires developers only to two simple Annotations declaring a Service but not regards how you
 * build the service component itself. the Service component is simply java class that implements the service Interface
 * and ScaleCube take care for the rest of the magic. it derived and influenced by Actor model and reactive and
 * streaming patters but does not force application developers to it. ScaleCube-Services is not yet-anther RPC system in
 * the sense its is cluster aware to provide:
 * <ul>
 * <li>location transparency and discovery of service instances.</li>
 * <li>fault tolerance using gossip and failure detection.</li>
 * <li>share nothing - fully distributed and decentralized architecture.</li>
 * <li>Provides fluent, java 8 lambda apis.</li>
 * <li>Embeddable and lightweight.</li>
 * <li>utilizes completable futures but primitives and messages can be used as well completable futures gives the
 * advantage of composing and chaining service calls and service results.</li>
 * <li>low latency</li>
 * <li>supports routing extensible strategies when selecting service end-points</li>
 * </ul>
 * 
 * <b>basic usage example:</b>
 * 
 * <pre>
 * {@code
 *    // Define a service interface and implement it:
 *    &#64; Service
 *    public interface GreetingService {
 *         &#64; ServiceMethod
 *         CompletableFuture<String> asyncGreeting(String string);
 *     }
 *
 *     public class GreetingServiceImpl implements GreetingService {
 *       &#64; Override
 *       public CompletableFuture<String> asyncGreeting(String name) {
 *         return CompletableFuture.completedFuture(" hello to: " + name);
 *       }
 *     }
 *
 *     // Build a microservices cluster instance:
 *     Microservices microservices = Microservices.builder()
 *          // Introduce GreetingServiceImpl pojo as a micro-service:
 *         .services(new GreetingServiceImpl())
 *         .build();
 *
 *     // Create microservice proxy to GreetingService.class interface:
 *     GreetingService service = microservices.proxy()
 *         .api(GreetingService.class)
 *         .create();
 *
 *     // Invoke the greeting service async:
 *     CompletableFuture<String> future = service.asyncGreeting("joe");
 *
 *     // handle completable success or error:
 *     future.whenComplete((result, ex) -> {
 *      if (ex == null) {
 *        // print the greeting:
 *         System.out.println(result);
 *       } else {
 *         // print the greeting:
 *         System.out.println(ex);
 *       }
 *     });
 * }
 * </pre>
 */

public class Microservices {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final Cluster cluster;

  private final ServiceRegistry serviceRegistry;

  private final ServiceProxyFactory proxyFactory;

  private final ServiceDispatcherFactory dispatcherFactory;

  private final ServiceCommunicator sender;

  private Metrics metrics;

  private Microservices(Cluster cluster, ServiceCommunicator sender, ServicesConfig services, Metrics metrics) {
    this.cluster = cluster;
    this.sender = sender;
    this.metrics = metrics;
    this.serviceRegistry = new ServiceRegistryImpl(this, services, metrics);

    this.dispatcherFactory = new ServiceDispatcherFactory(serviceRegistry);
    this.proxyFactory = new ServiceProxyFactory(this);
    new ServiceDispatcher(this);

    this.sender.listen()
        .filter(message -> message.header(ServiceHeaders.SERVICE_RESPONSE) != null)
        .subscribe(message -> ServiceResponse.handleReply(message));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Cluster cluster() {
    return this.cluster;
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  private <T> T createProxy(Class<T> serviceInterface, Class<? extends Router> router, Duration timeout) {
    return proxyFactory.createProxy(serviceInterface, router, timeout, metrics);
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public static final class Builder {

    private ServicesConfig servicesConfig = ServicesConfig.empty();

    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();

    private TransportConfig transportConfig = TransportConfig.defaultConfig();

    private Metrics metrics;

    /**
     * Microservices instance builder.
     * 
     * @return Microservices instance.
     */
    public Microservices build() {

      Cluster cluster = null;

      // create cluster and transport with given config.
      ServiceTransport transportSender =
          new ServiceTransport(Transport.bindAwait(transportConfig));

      ClusterConfig cfg = getClusterConfig(servicesConfig, transportSender.address());
      cluster = Cluster.joinAwait(cfg);
      transportSender.cluster(cluster);

      return Reflect.builder(new Microservices(cluster, transportSender, servicesConfig, this.metrics)).inject();
    }

    private ClusterConfig getClusterConfig(ServicesConfig servicesConfig, Address address) {
      if (servicesConfig != null && !servicesConfig.services().isEmpty()) {
        clusterConfig.addMetadata(Microservices.metadata(servicesConfig));
        if (address != null) {
          clusterConfig.addMetadata("service-address", address.toString());
        }
      }
      return clusterConfig.build();
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
      checkNotNull(transportConfig);
      this.transportConfig = transportConfig;
      return this;
    }

    public Builder metrics(MetricRegistry metrics) {
      checkNotNull(metrics);
      this.metrics = new Metrics(metrics);
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
      return dispatcherFactory.createDispatcher(this.router, this.timeout, metrics);
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

    config.services().stream().forEach(serviceConfig -> {

      serviceConfig.serviceNames().stream().forEach(name -> {

        servicesTags.put(new ServiceInfo(name,
            serviceConfig.methods(name),
            serviceConfig.getTags()).toMetadata(),
            "service");
      });
    });

    return servicesTags;
  }

  /**
   * returns service communication.
   * 
   * @return service communication.
   */
  public ServiceCommunicator sender() {
    return sender;
  }

  /**
   * Shutdown services transport and cluster transport.
   * 
   * @return future with cluster shutdown result.
   */
  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> result = new CompletableFuture<Void>();

    if (!this.sender.isStopped()) {
      this.sender.shutdown();
    }

    if (!this.cluster.isShutdown()) {
      return this.cluster.shutdown();
    } else {
      result.completeExceptionally(new IllegalStateException("Cluster transport alredy stopped"));
      return result;
    }
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }
}
