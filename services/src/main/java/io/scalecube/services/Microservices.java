package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.discovery.ServiceDiscovery;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.services.transport.DefaultServerMessageAcceptor;
import io.scalecube.services.transport.LocalServiceDispatchers;
import io.scalecube.services.transport.TransportFactory;
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


  public static final int SERVICE_PORT = 5801;

  private final ServiceRegistry serviceRegistry;

  private final ClientTransport client;

  private Metrics metrics;

  private Address serviceAddress;

  public RouterFactory routerFactory;

  private ServiceDiscovery discovery;

  private ServerTransport server;

  private final LocalServiceDispatchers localServices;

  private Microservices(ServerTransport server,
      ClientTransport client,
      ClusterConfig.Builder clusterConfig,
      List<ServiceWithTags> services,
      Map<String, ? extends ServiceMessageCodec> codecs,
      Metrics metrics) {

    // provision services for service access.
    this.metrics = metrics;
    this.client = client;
    this.server = server;

    localServices = LocalServiceDispatchers.builder()
        .services(services.stream().map(ServiceWithTags::service).collect(Collectors.toList())).build();

    if (services.size() > 0) {
      server.accept(new DefaultServerMessageAcceptor(localServices, codecs));
      InetSocketAddress inet = server.bindAwait(new InetSocketAddress(Addressing.getLocalIpAddress(), 0));
      serviceAddress = Address.create(inet.getHostString(), inet.getPort());
    } else {
      serviceAddress = Address.from("localhost:0");
    }

    ServiceEndpoint localServiceEndpoint = ServiceScanner.scan(
        // TODO: pass tags as well [sergeyr]
        services,
        serviceAddress.host(),
        serviceAddress.port(),
        new HashMap<>());
    // register and make them discover-able

    serviceRegistry = new ServiceRegistryImpl();
    serviceRegistry.registerService(localServiceEndpoint);

    routerFactory = new RouterFactory(serviceRegistry);

    discovery = new ServiceDiscovery(serviceRegistry);
    discovery.start(clusterConfig);
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Collection<Object> services() {
    return localServices.services();
  }

  public Collection<ServiceEndpoint> serviceEndpoints() {
    return serviceRegistry.listServiceEndpoints();
  }

  public static final class Builder {

    private List<ServiceWithTags> services = new ArrayList<>();
    private ClusterConfig.Builder clusterConfig = ClusterConfig.builder();
    private Metrics metrics;
    private ServerTransport server = TransportFactory.getTransport().getServerTransport();
    private ClientTransport client = TransportFactory.getTransport().getClientTransport();
    private Map<String, ? extends ServiceMessageCodec> codecs = TransportFactory.getTransport().getMessageCodecs();

    /**
     * Microservices instance builder.
     *
     * @return Microservices instance.
     */
    public Microservices build() {
      return Reflect
          .builder(new Microservices(server, client, clusterConfig, services, codecs, metrics))
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

    public Builder metrics(MetricRegistry metrics) {
      checkNotNull(metrics);
      this.metrics = new Metrics(metrics);
      return this;
    }

    public Builder services(Object... services) {
      checkNotNull(services);
      this.services = Arrays.stream(services).map(ServiceWithTags::new).collect(Collectors.toList());
      return this;
    }

    public ServiceWithTagsBuilder withService(Object serviceInstance) {
      return new ServiceWithTagsBuilder(serviceInstance, this);
    }
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

  public Router router(Class<? extends Router> routerType) {
    return routerFactory.getRouter(routerType);
  }

  public Call call() {
    Router router = this.router(RoundRobinServiceRouter.class);
    return new ServiceCall(client, localServices).call().metrics(metrics).router(router);
  }

  public Mono<Void> shutdown() {
    return Mono.when(Mono.fromFuture(discovery.shutdown()), server.stop());
  }

  public Cluster cluster() {
    return discovery.cluster();
  }

  public static class ServiceWithTagsBuilder {
    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();
    private final Builder that;

    ServiceWithTagsBuilder(Object serviceInstance, Builder that) {
      this.serviceInstance = serviceInstance;
      this.that = that;
    }

    public ServiceWithTagsBuilder withTag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    public Builder register() {
      that.services.add(new ServiceWithTags(serviceInstance, tags));
      return that;
    }
  }

  public static class ServiceWithTags {

    private final Object serviceInstance;
    private final Map<String, String> tags;

    ServiceWithTags(Object serviceInstance) {
      this(serviceInstance, Collections.emptyMap());
    }

    ServiceWithTags(Object serviceInstance, Map<String, String> tags) {
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
