package io.scalecube.services;

import static io.scalecube.services.discovery.ServiceDiscovery.SERVICE_METADATA;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.discovery.ServiceDiscovery;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;

import com.codahale.metrics.MetricRegistry;

import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
  private final ServiceDiscovery discovery;
  private final ServerTransport server;
  private final ServiceMethodRegistry methodRegistry;
  private final List<ServiceInfo> services;
  private final ClusterConfig.Builder clusterConfig;
  private final String id;
  private final int servicePort;

  private Address serviceAddress; // calculated
  private Cluster cluster; // calculated

  private Microservices(Builder builder) {
    this.id = IdGenerator.generateId();
    this.servicePort = builder.servicePort;
    this.metrics = builder.metrics;
    this.client = builder.client;
    this.server = builder.server;
    this.clusterConfig = builder.clusterConfig;
    this.serviceRegistry = builder.serviceRegistry;
    this.services = Collections.unmodifiableList(new ArrayList<>(builder.services));
    this.methodRegistry = builder.methodRegistry;
    this.discovery = new ServiceDiscovery(serviceRegistry);
  }

  public String id() {
    return this.id;
  }

  private Mono<Microservices> start() {
    // register service in method registry
    services.stream().map(ServiceInfo::serviceInstance).forEach(methodRegistry::registerService);

    // bind service server transport
    InetSocketAddress address =
        InetSocketAddress.createUnresolved(Addressing.getLocalIpAddress().getHostAddress(), servicePort);
    InetSocketAddress boundAddress = server.bindAwait(address, methodRegistry);
    serviceAddress = Address.create(boundAddress.getHostString(), boundAddress.getPort());

    // register services in service registry
    if (!services.isEmpty()) {
      // TODO: pass tags as well [sergeyr]
      serviceRegistry.registerService(
          ServiceScanner.scan(services, id, serviceAddress.host(), serviceAddress.port(), new HashMap<>()));
    }

    // setup cluster metadata
    clusterConfig.addMetadata(serviceRegistry.listServiceEndpoints().stream()
        .collect(Collectors.toMap(ServiceDiscovery::encodeMetadata, service -> SERVICE_METADATA)));

    return Mono.fromFuture(Cluster.join(clusterConfig.build())).map(this::init);
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Collection<Object> services() {
    return services.stream().map(ServiceInfo::serviceInstance).collect(Collectors.toList());
  }

  public static final class Builder {

    private int servicePort = 0;
    private List<ServiceInfo> services = new ArrayList<>();
    private List<Function<Call, Collection<Object>>> serviceProviders = new ArrayList<>();
    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();
    private Metrics metrics;
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private ServerTransport server = ServiceTransport.getTransport().getServerTransport();
    private ClientTransport client = ServiceTransport.getTransport().getClientTransport();

    public Mono<Microservices> start() {
      Call call = new Call(client, methodRegistry, serviceRegistry).metrics(this.metrics);

      serviceProviders.stream()
          .flatMap(provider -> provider.apply(call).stream())
          .forEach(service -> services.add(
              service instanceof ServiceInfo ? //
                  ((ServiceInfo) service)
                  : ServiceInfo.fromServiceInstance(service).build()));

      return new Microservices(this).start();
    }

    public Microservices startAwait() {
      return start().block();
    }

    public Builder services(Object... services) {
      serviceProviders.add(call -> Arrays.stream(services).collect(Collectors.toList()));
      return this;
    }

    public Builder services(Function<Call, Collection<Object>> serviceProvider) {
      serviceProviders.add(serviceProvider);
      return this;
    }

    public Builder serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public Builder methodRegistry(ServiceMethodRegistry methodRegistry) {
      this.methodRegistry = methodRegistry;
      return this;
    }

    public Builder server(ServerTransport server) {
      this.server = server;
      return this;
    }

    public Builder client(ClientTransport client) {
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
      this.clusterConfig.seedMembers(seeds);
      return this;
    }

    public Builder clusterConfig(ClusterConfig.Builder clusterConfig) {
      this.clusterConfig = clusterConfig;
      return this;
    }

    public Builder metrics(MetricRegistry metrics) {
      this.metrics = new Metrics(metrics);
      return this;
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
    return new Call(client, methodRegistry, serviceRegistry).metrics(metrics);
  }

  public Mono<Void> shutdown() {
    return Mono.when(Mono.fromFuture(cluster.shutdown()), server.stop(), serviceRegistry.shutdown());
  }

  public Cluster cluster() {
    return cluster;
  }

}
