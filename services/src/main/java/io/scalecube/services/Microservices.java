package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.services.transport.TransportFactory;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import com.codahale.metrics.MetricRegistry;

import java.util.Collection;
import java.util.Objects;
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
 *     GreetingService service = microservices.call()
 *         .api(GreetingService.class);
 *
 *     // Invoke the greeting service async:
 *     CompletableFuture<String> future = service.sayHello("joe");
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

  private final Cluster cluster;

  private final ServiceRegistry serviceRegistry;

  private final ServiceCall client;
  private final ServerTransport server;

  private Metrics metrics;

  private Address serviceAddress;

  public RouterFactory routerFactory;


  private Microservices(ServerTransport server,
      ClientTransport client,
      ClusterConfig.Builder clusterConfig,
      Services services,
      Metrics metrics) {

    this.client = new ServiceCall(client);
    this.metrics = metrics;
    this.serviceRegistry = new ServiceRegistryImpl();
    this.routerFactory = new RouterFactory(serviceRegistry);

    services.stream().forEach(service -> {
      this.serviceRegistry.registerService(service);
    });

    this.server = server.services(services);
    this.serviceAddress = server.bindAwait(5801);
    
    Cluster.joinAwait(config)
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Cluster cluster() {
    return this.cluster;
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public static final class Builder {

    private Services servicesConfig = Services.empty();

    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();

    private Metrics metrics;

    private ServerTransport server = TransportFactory.getTransport().getServerTransport();
    private ClientTransport client = TransportFactory.getTransport().getClientTransport();

    private Services services;

    /**
     * Microservices instance builder.
     *
     * @return Microservices instance.
     */
    public Microservices build() {

      Objects.requireNonNull(this.server,
          "SPI ServiceTransport is missing and not found by ServiceLoader."
              + " did you forget to set Transport provider?");

      return Reflect.builder(
          new Microservices(this.server, this.client, clusterConfig, servicesConfig, this.metrics))
          .inject();

    }

    public Builder server(ServerTransport server) {
      this.server = server;
      return this;
    }

    public Builder client(ClientTransport client) {
      this.client = client;
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
    public Builder services(Services services) {
      checkNotNull(services);
      this.services = services;
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

  /**
   * Shutdown services transport and cluster transport.
   *
   * @return future with cluster shutdown result.
   */
  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> result = new CompletableFuture<Void>();

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

  public Address serviceAddress() {
    return this.serviceAddress;
  }

  public Router router(Class<? extends Router> routerType) {
    return routerFactory.getRouter(routerType);
  }

  public Call call() {
    Router router = this.router(RoundRobinServiceRouter.class);
    return client.call().metrics(metrics).router(router);
  }
}
