package io.scalecube.services;

import static java.util.stream.Collectors.toMap;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryConfig;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.ServiceTransportConfig;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
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
  private final List<ServiceInfo> serviceInfos = new ArrayList<>();
  private final List<Function<Call, Collection<Object>>> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscovery discovery;
  private final Consumer<ServiceDiscoveryConfig.Builder> discoveryOptions;

  private Microservices(Builder builder) {
    this.id = UUID.randomUUID().toString();
    this.metrics = builder.metrics;
    this.tags = new HashMap<>(builder.tags);
    this.serviceProviders = new ArrayList<>(builder.serviceProviders);
    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discovery = builder.discovery;
    this.discoveryOptions = builder.discoveryOptions;
    this.transportBootstrap =
        new ServiceTransportBootstrap(
            ServiceTransportConfig.builder(builder.transportOptions).build());
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return this.id;
  }

  private Mono<Microservices> start() {
    return transportBootstrap
        .start(methodRegistry)
        .flatMap(
            input -> {
              ClientTransport clientTransport = transportBootstrap.clientTransport();
              InetSocketAddress serviceAddress = transportBootstrap.serviceAddress();

              Call call = new Call(clientTransport, methodRegistry, serviceRegistry);

              // invoke service providers and register services
              serviceProviders
                  .stream()
                  .flatMap(serviceProvider -> serviceProvider.apply(call).stream())
                  .forEach(this::collectAndRegister);

              // register services in service registry
              ServiceEndpoint endpoint = null;
              if (!serviceInfos.isEmpty()) {
                String serviceHost = serviceAddress.getHostString();
                int servicePort = serviceAddress.getPort();
                endpoint = ServiceScanner.scan(serviceInfos, id, serviceHost, servicePort, tags);
                serviceRegistry.registerService(endpoint);
              }

              // configure discovery and publish to the cluster
              ServiceDiscoveryConfig discoveryConfig =
                  ServiceDiscoveryConfig.builder(discoveryOptions)
                      .serviceRegistry(serviceRegistry)
                      .endpoint(endpoint)
                      .build();
              return discovery
                  .start(discoveryConfig)
                  .then(Mono.defer(this::doInjection))
                  .then(Mono.defer(() -> startGateway(call)))
                  .then(Mono.just(this));
            })
        .onErrorResume(
            ex -> {
              // return original error then shutdown
              return Mono.when(Mono.error(ex), shutdown()).cast(Microservices.class);
            });
  }

  private Mono<GatewayBootstrap> startGateway(Call call) {
    Executor workerThreadPool = transportBootstrap.workerThreadPool();
    boolean preferNative = transportBootstrap.transport().isNativeSupported();
    return gatewayBootstrap.start(workerThreadPool, preferNative, call, metrics);
  }

  private Mono<Microservices> doInjection() {
    List<Object> serviceInstances =
        serviceInfos.stream().map(ServiceInfo::serviceInstance).collect(Collectors.toList());
    return Mono.just(Reflect.inject(this, serviceInstances));
  }

  private void collectAndRegister(Object serviceInstance) {
    // collect
    ServiceInfo serviceInfo =
        serviceInstance instanceof ServiceInfo
            ? ((ServiceInfo) serviceInstance)
            : ServiceInfo.fromServiceInstance(serviceInstance).build();
    serviceInfos.add(serviceInfo);
    // register service object
    methodRegistry.registerService(serviceInfo.serviceInstance());
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public InetSocketAddress serviceAddress() {
    return transportBootstrap.serviceAddress();
  }

  public Call call() {
    return new Call(transportBootstrap.clientTransport(), methodRegistry, serviceRegistry);
  }

  public InetSocketAddress gatewayAddress(String name, Class<? extends Gateway> gatewayClass) {
    return gatewayBootstrap.gatewayAddress(name, gatewayClass);
  }

  public Map<GatewayConfig, InetSocketAddress> gatewayAddresses() {
    return gatewayBootstrap.gatewayAddresses();
  }

  public ServiceDiscovery discovery() {
    return this.discovery;
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.defer(
        () ->
            Mono.when(
                Optional.ofNullable(discovery).map(ServiceDiscovery::shutdown).orElse(Mono.empty()),
                Optional.ofNullable(gatewayBootstrap)
                    .map(GatewayBootstrap::shutdown)
                    .orElse(Mono.empty()),
                Optional.ofNullable(transportBootstrap)
                    .map(ServiceTransportBootstrap::shutdown)
                    .orElse(Mono.empty())));
  }

  public static final class Builder {

    private Metrics metrics;
    private Map<String, String> tags = new HashMap<>();
    private List<Function<Call, Collection<Object>>> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private ServiceDiscovery discovery = ServiceDiscovery.getDiscovery();
    private Consumer<ServiceDiscoveryConfig.Builder> discoveryOptions;
    private Consumer<ServiceTransportConfig.Builder> transportOptions;
    private GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();

    public Mono<Microservices> start() {
      return Mono.defer(() -> new Microservices(this).start());
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

    public Builder discovery(Consumer<ServiceDiscoveryConfig.Builder> discoveryOptions) {
      this.discoveryOptions = discoveryOptions;
      return this;
    }

    public Builder transport(Consumer<ServiceTransportConfig.Builder> transportOptions) {
      this.transportOptions = transportOptions;
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

    private Set<GatewayConfig> gatewayConfigs = new HashSet<>(); // config
    private Map<GatewayConfig, Gateway> gatewayInstances = new HashMap<>(); // calculated

    private GatewayBootstrap addConfig(GatewayConfig config) {
      if (!gatewayConfigs.add(config)) {
        throw new IllegalArgumentException(
            "GatewayConfig with name: '"
                + config.name()
                + "' and gatewayClass: '"
                + config.gatewayClass().getName()
                + "' was already defined");
      }
      return this;
    }

    private Mono<GatewayBootstrap> start(
        Executor workerThreadPool, boolean preferNative, Call call, Metrics metrics) {
      return Flux.fromIterable(gatewayConfigs)
          .flatMap(
              gatewayConfig ->
                  Gateway.getGateway(gatewayConfig.gatewayClass())
                      .start(gatewayConfig, workerThreadPool, preferNative, call, metrics)
                      .doOnSuccess(gw -> gatewayInstances.put(gatewayConfig, gw)))
          .then(Mono.just(this));
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              gatewayInstances != null && !gatewayInstances.isEmpty()
                  ? Mono.when(
                      gatewayInstances.values().stream().map(Gateway::stop).toArray(Mono[]::new))
                  : Mono.empty());
    }

    private InetSocketAddress gatewayAddress(String name, Class<? extends Gateway> gatewayClass) {
      Optional<GatewayConfig> result =
          gatewayInstances
              .keySet()
              .stream()
              .filter(config -> config.name().equals(name))
              .filter(config -> config.gatewayClass() == gatewayClass)
              .findFirst();

      if (!result.isPresent()) {
        throw new IllegalArgumentException(
            "Didn't find gateway address under name: '"
                + name
                + "' and gateway class: '"
                + gatewayClass.getName()
                + "'");
      }

      return gatewayInstances.get(result.get()).address();
    }

    private Map<GatewayConfig, InetSocketAddress> gatewayAddresses() {
      return Collections.unmodifiableMap(
          gatewayInstances
              .entrySet()
              .stream()
              .collect(toMap(Entry::getKey, e -> e.getValue().address())));
    }
  }

  private static class ServiceTransportBootstrap {

    private static final int DEFAULT_NUM_OF_THREADS = Runtime.getRuntime().availableProcessors();

    private String serviceHost; // config
    private int servicePort; // config
    private ServiceTransport transport; // config or calculated
    private ClientTransport clientTransport; // calculated
    private ServerTransport serverTransport; // calculated
    private Executor workerThreadPool; // calculated
    private InetSocketAddress serviceAddress; // calculated
    private int numOfThreads; // calculated

    public ServiceTransportBootstrap(ServiceTransportConfig options) {
      this.serviceHost = options.host();
      this.servicePort = Optional.ofNullable(options.port()).orElse(0);
      this.numOfThreads =
          Optional.ofNullable(options.numOfThreads()).orElse(DEFAULT_NUM_OF_THREADS);
      this.transport = options.transport();
    }

    private ServiceTransport transport() {
      return transport;
    }

    private ClientTransport clientTransport() {
      return clientTransport;
    }

    private Executor workerThreadPool() {
      return workerThreadPool;
    }

    private InetSocketAddress serviceAddress() {
      return serviceAddress;
    }

    private Mono<ServiceTransportBootstrap> start(ServiceMethodRegistry methodRegistry) {
      return Mono.defer(
          () -> {
            this.transport =
                Optional.ofNullable(this.transport).orElseGet(ServiceTransport::getTransport);

            this.workerThreadPool = transport.getWorkerThreadPool(numOfThreads);
            this.clientTransport = transport.getClientTransport(workerThreadPool);
            this.serverTransport = transport.getServerTransport(workerThreadPool);

            // bind service serverTransport transport
            return serverTransport
                .bind(servicePort, methodRegistry)
                .map(
                    listenAddress -> {
                      // prepare service host:port for exposing
                      int port = listenAddress.getPort();
                      String host =
                          Optional.ofNullable(serviceHost)
                              .orElseGet(() -> Address.getLocalIpAddress().getHostAddress());
                      this.serviceAddress = InetSocketAddress.createUnresolved(host, port);
                      return this;
                    });
          });
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Mono.when(
                  Optional.ofNullable(serverTransport)
                      .map(ServerTransport::stop)
                      .orElse(Mono.empty()),
                  Optional.ofNullable(transport)
                      .map(transport -> transport.shutdown(workerThreadPool))
                      .orElse(Mono.empty())));
    }
  }
}
