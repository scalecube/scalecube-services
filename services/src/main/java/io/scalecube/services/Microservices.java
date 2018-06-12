package io.scalecube.services;

import static io.scalecube.services.discovery.ServiceDiscovery.SERVICE_METADATA;
import static java.util.Objects.requireNonNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.discovery.ServiceDiscovery;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.LocalServiceHandlers;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import com.codahale.metrics.MetricRegistry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import reactor.core.publisher.Mono;

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
 *         Mono<String> asyncGreeting(String string);
 *     }
 *
 *     public class GreetingServiceImpl implements GreetingService {
 *       &#64; Override
 *       public Mono<String> asyncGreeting(String name) {
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
 *     Mono<String> future = service.sayHello("joe");
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

  private final ServiceRegistry serviceRegistry;
  private final ClientTransport client;
  private final Metrics metrics;
  private final Address serviceAddress;
  private final ServiceDiscovery discovery;
  private final ServerTransport server;
  private final LocalServiceHandlers serviceHandlers;
  private final List<Object> services;
  private final ClusterConfig.Builder clusterConfig;

  private Cluster cluster; // calculated field

  private Microservices(Builder builder) {

    // provision services for service access.
    this.metrics = builder.metrics;
    this.client = builder.client;
    this.server = builder.server;

    this.services = builder.services.stream().map(mapper -> mapper.serviceInstance).collect(Collectors.toList());
    this.serviceHandlers = LocalServiceHandlers.builder()
        .services(builder.services.stream().map(ServiceInfo::service).collect(Collectors.toList())).build();

    InetSocketAddress socketAddress = new InetSocketAddress(Addressing.getLocalIpAddress(), builder.servicePort);
    InetSocketAddress address = server.bindAwait(socketAddress, serviceHandlers);
    serviceAddress = Address.create(address.getHostString(), address.getPort());

    serviceRegistry = new ServiceRegistryImpl();

    if (services.size() > 0) {
      // TODO: pass tags as well [sergeyr]
      serviceRegistry.registerService(ServiceScanner.scan(
          builder.services,
          serviceAddress.host(),
          serviceAddress.port(),
          new HashMap<>()));
    }

    discovery = new ServiceDiscovery(serviceRegistry);

    clusterConfig = builder.clusterConfig;
  }

  private Mono<Microservices> start() {
    clusterConfig.addMetadata(serviceRegistry.listServiceEndpoints().stream()
        .collect(Collectors.toMap(ServiceDiscovery::encodeMetadata, service -> SERVICE_METADATA)));
    return Mono.fromFuture(Cluster.join(clusterConfig.build())).map(this::init);
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Collection<Object> services() {
    return services;
  }

  public Collection<ServiceEndpoint> serviceEndpoints() {
    return serviceRegistry.listServiceEndpoints();
  }

  public static final class Builder {

    public int servicePort = 0;
    private List<ServiceInfo> services = new ArrayList<>();
    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();
    private Metrics metrics;
    private ServerTransport server = ServiceTransport.getTransport().getServerTransport();
    private ClientTransport client = ServiceTransport.getTransport().getClientTransport();

    /**
     * Microservices instance builder.
     *
     * @return Mono<Microservices> instance.
     */
    public Mono<Microservices> start() {
      Microservices instance = new Microservices(this);
      return instance.start();
    }

    /**
     * Microservices instance builder.
     *
     * @return Microservices instance.
     */
    public Microservices startAwait() {
      return new Microservices(this).start().block();
    }

    public Builder server(ServerTransport server) {
      requireNonNull(server);
      this.server = server;
      return this;
    }

    public Builder client(ClientTransport client) {
      requireNonNull(client);
      this.client = client;
      return this;
    }

    public Builder discoveryPort(int port) {
      this.clusterConfig.port(port);
      return this;
    }

    public Builder servicePort(int port) {
      this.servicePort = port;
      return this;
    }

    public Builder seeds(Address... seeds) {
      requireNonNull(seeds);
      this.clusterConfig.seedMembers(seeds);
      return this;
    }

    public Builder clusterConfig(ClusterConfig.Builder clusterConfig) {
      requireNonNull(clusterConfig);
      this.clusterConfig = clusterConfig;
      return this;
    }

    public Builder metrics(MetricRegistry metrics) {
      requireNonNull(metrics);
      this.metrics = new Metrics(metrics);
      return this;
    }

    public Builder services(Object... services) {
      requireNonNull(services);
      this.services = Arrays.stream(services).map(ServiceInfo::new).collect(Collectors.toList());
      return this;
    }

    public ServiceBuilder service(Object serviceInstance) {
      requireNonNull(serviceInstance);
      return new ServiceBuilder(serviceInstance, this);
    }
  }

  private Microservices init(Cluster cluster) {
    this.cluster = cluster;
    discovery.init(cluster);
    return Reflect.builder(this).inject();
  }

  public static Builder builder() {
    return new Builder();
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }

  public Address serviceAddress() {
    return this.serviceAddress;
  }

  public Call call() {
    Router router = Routers.getRouter(RoundRobinServiceRouter.class);
    return new Call(client, serviceHandlers, serviceRegistry).metrics(metrics).router(router);
  }

  public Mono<Void> shutdown() {
    return Mono.when(Mono.fromFuture(cluster.shutdown()), server.stop());
  }

  public Cluster cluster() {
    return cluster;
  }

  public static class ServiceBuilder {
    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();
    private final Builder that;

    ServiceBuilder(Object serviceInstance, Builder that) {
      this.serviceInstance = serviceInstance;
      this.that = that;
    }

    public ServiceBuilder tag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    public Builder register() {
      that.services.add(new ServiceInfo(serviceInstance, tags));
      return that;
    }
  }

  public static class ServiceInfo {

    private final Object serviceInstance;
    private final Map<String, String> tags;

    public ServiceInfo(Object serviceInstance) {
      this(serviceInstance, Collections.emptyMap());
    }

    public ServiceInfo(Object serviceInstance, Map<String, String> tags) {
      this.serviceInstance = serviceInstance;
      this.tags = tags;
    }

    public Object service() {
      return serviceInstance;
    }

    public Map<String, String> tags() {
      return tags;
    }
  }
}
