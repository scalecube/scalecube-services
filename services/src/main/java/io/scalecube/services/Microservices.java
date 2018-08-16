package io.scalecube.services;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.discovery.api.DiscoveryConfig;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.transport.Address;
import io.scalecube.transport.Addressing;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The ScaleCube-Services module enables to provision and consuming microservices in a cluster.
 * ScaleCube-Services provides Reactive application development platform for building distributed
 * applications Using microservices and fast data on a message-driven runtime that scales
 * transparently on multi-core, multi-process and/or multi-machines Most microservices frameworks
 * focus on making it easy to build individual microservices. ScaleCube allows developers to run a
 * whole system of microservices from a single command. removing most of the boilerplate code,
 * ScaleCube-Services focuses development on the essence of the service and makes it easy to create
 * explicit and typed protocols that compose. True isolation is achieved through shared-nothing
 * design. This means the services in ScaleCube are autonomous, loosely coupled and mobile (location
 * transparent)—necessary requirements for resilance and elasticity ScaleCube services requires
 * developers only to two simple Annotations declaring a Service but not regards how you build the
 * service component itself. the Service component is simply java class that implements the service
 * Interface and ScaleCube take care for the rest of the magic. it derived and influenced by Actor
 * model and reactive and streaming patters but does not force application developers to it.
 * ScaleCube-Services is not yet-anther RPC system in the sense its is cluster aware to provide:
 *
 * <ul>
 *   <li>location transparency and discovery of service instances.
 *   <li>fault tolerance using gossip and failure detection.
 *   <li>share nothing - fully distributed and decentralized architecture.
 *   <li>Provides fluent, java 8 lambda apis.
 *   <li>Embeddable and lightweight.
 *   <li>utilizes completable futures but primitives and messages can be used as well completable
 *       futures gives the advantage of composing and chaining service calls and service results.
 *   <li>low latency
 *   <li>supports routing extensible strategies when selecting service end-points
 * </ul>
 *
 * <b>basic usage example:</b>
 *
 * <pre>{@code
 * // Define a service interface and implement it:
 * &#64; Service
 * public interface GreetingService {
 *      &#64; ServiceMethod
 *      Mono<String> sayHello(String string);
 *  }
 *
 *  public class GreetingServiceImpl implements GreetingService {
 *    &#64; Override
 *    public Mono<String> sayHello(String name) {
 *      return Mono.just("hello to: " + name);
 *    }
 *  }
 *
 *  // Build a microservices cluster instance:
 *  Microservices microservices = Microservices.builder()
 *       // Introduce GreetingServiceImpl pojo as a micro-service:
 *      .services(new GreetingServiceImpl())
 *      .startAwait();
 *
 *  // Create microservice proxy to GreetingService.class interface:
 *  GreetingService service = microservices.call().create()
 *      .api(GreetingService.class);
 *
 *  // Invoke the greeting service async:
 *  service.sayHello("joe").subscribe(resp->{
 *    // handle response
 *  });
 *
 * }</pre>
 */
public class Microservices {

  private final String id;
  private final Metrics metrics;
  private final Map<String, String> tags;
  private final List<ServiceInfo> services;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final DiscoveryConfig.Builder discoveryConfig;
  private final ServiceDiscovery discovery;

  private Microservices(Builder builder) {
    this.id = IdGenerator.generateId();
    this.metrics = builder.metrics;
    this.tags = new HashMap<>(builder.tags);
    this.services = new ArrayList<>(builder.services);
    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;

    this.transportBootstrap = builder.transportBootstrap;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discovery = builder.discovery;
    this.discoveryConfig = builder.discoveryConfig;
  }

  public String id() {
    return this.id;
  }

  private Mono<Microservices> start() {
    List<Object> serviceInstances =
        services.stream().map(ServiceInfo::serviceInstance).collect(Collectors.toList());

    // register service in method registry
    serviceInstances.forEach(methodRegistry::registerService);

    // register services in service registry
    if (!services.isEmpty()) {
      InetSocketAddress serviceAddress = transportBootstrap.listenAddress();
      String serviceHost = serviceAddress.getHostString();
      int servicePort = serviceAddress.getPort();

      ServiceEndpoint endpoint =
          ServiceScanner.scan(services, id, serviceHost, servicePort, tags);

      serviceRegistry.registerService(endpoint);
      discoveryConfig.endpoint(endpoint);
    }

    // configure discovery and publish to the cluster.
    return discovery
        .start(discoveryConfig.serviceRegistry(serviceRegistry).build())
        .map(discovery -> (this.discovery = discovery))
        .then(Mono.just(Reflect.inject(this, serviceInstances)))
        .then(gatewayBootstrap.start().then(Mono.just(this)));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public static final class Builder {

    private Metrics metrics;
    private Map<String, String> tags = new HashMap<>();
    private List<ServiceInfo> services = new ArrayList<>();
    private List<Function<Call, Collection<Object>>> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private ServiceDiscovery discovery = ServiceDiscovery.getDiscovery();
    private DiscoveryConfig.Builder discoveryConfig = DiscoveryConfig.builder();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();

    public Mono<Microservices> start() {
      return Mono.defer(() -> transportBootstrap.start(methodRegistry))
          .map(ServiceTransportBootstrap::clientTransport)
          .flatMap(
              clientTransport -> {
                Call call =
                    new Call(clientTransport, methodRegistry, serviceRegistry)
                        .metrics(this.metrics);

                serviceProviders
                    .stream()
                    .flatMap(provider -> provider.apply(call).stream())
                    .forEach(
                        service ->
                            services.add(
                                service instanceof ServiceInfo
                                    ? ((ServiceInfo) service)
                                    : ServiceInfo.fromServiceInstance(service).build()));

                return new Microservices(this).start();
              });
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

    public Builder discovery(ServiceDiscovery discovery) {
      this.discovery = discovery;
      return this;
    }

    public Builder transport(ServiceTransport transport) {
      this.transportBootstrap.transport(transport);
      return this;
    }

    public Builder discoveryPort(int port) {
      this.discoveryConfig.port(port);
      return this;
    }

    public Builder servicePort(int port) {
      this.transportBootstrap.listenPort(port);
      return this;
    }

    public Builder seeds(Address... seeds) {
      this.discoveryConfig.seeds(seeds);
      return this;
    }

    public Builder discoveryConfig(DiscoveryConfig.Builder discoveryConfig) {
      this.discoveryConfig = discoveryConfig;
      return this;
    }

    public Builder metrics(MetricRegistry metrics) {
      this.metrics = new Metrics(metrics);
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder gateway(GatewayConfig config) {
      gatewayBootstrap.addConfig(config);
      return this;
    }
  }

  private static class GatewayBootstrap {

    private List<GatewayConfig> gatewayConfigs = new ArrayList<>(); // config
    private List<Gateway> gatewayInstances = new ArrayList<>(); // calculated
    private Map<Class<? extends Gateway>, InetSocketAddress> gatewayAddresses =
        new HashMap<>(); // calculated

    private GatewayBootstrap addConfig(GatewayConfig config) {
      gatewayConfigs.add(config);
      return this;
    }

    private Mono<GatewayBootstrap> start(
        ExecutorService executorService, Call call, Metrics metrics) {
      return Flux.fromIterable(gatewayConfigs)
          .flatMap(
              gatewayConfig -> {
                Class<? extends Gateway> gatewayClass = gatewayConfig.gatewayClass();
                Gateway gateway = Gateway.getGateway(gatewayClass);
                gatewayInstances.add(gateway);
                return gateway
                    .start(gatewayConfig, executorService, call, metrics)
                    .doOnSuccess(address -> gatewayAddresses.put(gatewayClass, address));
              })
          .then(Mono.just(this));
    }

    private Mono<Void> shutdown() {
      return Flux.fromIterable(gatewayInstances).flatMap(Gateway::stop).then();
    }

    private InetSocketAddress gatewayAddress(Class<? extends Gateway> gatewayClass) {
      return gatewayAddresses.get(gatewayClass);
    }

    private Map<Class<? extends Gateway>, InetSocketAddress> gatewayAddresses() {
      return Collections.unmodifiableMap(gatewayAddresses);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }

  public Address serviceAddress() {
    InetSocketAddress listenAddress = transportBootstrap.listenAddress();
    return Address.create(listenAddress.getHostString(), listenAddress.getPort());
  }

  public Call call() {
    return new Call(transportBootstrap.clientTransport(), methodRegistry, serviceRegistry)
        .metrics(metrics);
  }

  public InetSocketAddress gatewayAddress(Class<? extends Gateway> gatewayClass) {
    return gatewayBootstrap.gatewayAddress(gatewayClass);
  }

  public Map<Class<? extends Gateway>, InetSocketAddress> gatewayAddresses() {
    return gatewayBootstrap.gatewayAddresses();
  }

  public ServiceDiscovery discovery() {
    return this.discovery;
  }

  public Mono<Void> shutdown() {
    return Mono.when(
        gatewayBootstrap.shutdown(), discovery.shutdown(), transportBootstrap.shutdown());
  }

  private static class ServiceTransportBootstrap {

    private int listenPort; // config
    private ServiceTransport transport; // config or calculated
    private ClientTransport clientTransport; // calculated
    private ServerTransport serverTransport; // calculated
    private ExecutorService executorService; // calculated
    private InetSocketAddress listenAddress; // calculated

    private ServiceTransportBootstrap listenPort(int listenPort) {
      this.listenPort = listenPort;
      return this;
    }

    private ServiceTransportBootstrap transport(ServiceTransport transport) {
      this.transport = transport;
      return this;
    }

    private Mono<ServiceTransportBootstrap> start(ServiceMethodRegistry methodRegistry) {
      return Mono.defer(
          () -> {
            this.transport =
                Optional.ofNullable(this.transport).orElseGet(ServiceTransport::getTransport);

            this.executorService = transport.getExecutorService();
            this.clientTransport = transport.getClientTransport(executorService);
            this.serverTransport = transport.getServerTransport(executorService);

            // bind service serverTransport transport
            String hostAddress = Addressing.getLocalIpAddress().getHostAddress();
            InetSocketAddress socketAddress =
                InetSocketAddress.createUnresolved(hostAddress, listenPort);
            this.listenAddress = serverTransport.bindAwait(socketAddress, methodRegistry);

            return Mono.just(this);
          });
    }

    private Mono<Void> shutdown() {
      return Mono.when(serverTransport.stop(), transport.shutdown(executorService));
    }

    private ClientTransport clientTransport() {
      return clientTransport;
    }

    private ExecutorService executorService() {
      return executorService;
    }

    private InetSocketAddress listenAddress() {
      return listenAddress;
    }
  }
}
