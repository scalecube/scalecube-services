package io.scalecube.services;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.discovery.api.ServiceDiscoveryOptions;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
 *  GreetingService service = microservices.call()
 *      .api(GreetingService.class);
 *
 *  // Invoke the greeting service async:
 *  service.sayHello("joe").subscribe(resp->{
 *    // handle response
 *  });
 *
 * }</pre>
 */
public final class Microservices implements AutoCloseable {

  public static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final String id = UUID.randomUUID().toString();
  private final Map<String, String> tags;
  private final List<ServiceProvider> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final Authenticator<Object> defaultAuthenticator;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscoveryBootstrap discoveryBootstrap;
  private final ServiceProviderErrorMapper defaultErrorMapper;
  private final ServiceMessageDataDecoder defaultDataDecoder;
  private final String defaultContentType;
  private final PrincipalMapper<Object, Object> defaultPrincipalMapper;
  private final Sinks.One<Void> shutdown = Sinks.one();
  private final Sinks.One<Void> onShutdown = Sinks.one();
  private ServiceEndpoint serviceEndpoint;
  private final String externalHost;
  private final Integer externalPort;

  private Microservices(Builder builder) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.serviceProviders = new ArrayList<>(builder.serviceProviders);
    this.serviceRegistry = builder.serviceRegistry;
    this.defaultAuthenticator = builder.defaultAuthenticator;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discoveryBootstrap = builder.discoveryBootstrap;
    this.transportBootstrap = builder.transportBootstrap;
    this.defaultErrorMapper = builder.defaultErrorMapper;
    this.defaultDataDecoder = builder.defaultDataDecoder;
    this.defaultContentType = builder.defaultContentType;
    this.defaultPrincipalMapper = builder.defaultPrincipalMapper;
    this.externalHost = builder.externalHost;
    this.externalPort = builder.externalPort;

    // Setup cleanup
    shutdown
        .asMono()
        .then(doShutdown())
        .doFinally(s -> onShutdown.emitEmpty(RETRY_NON_SERIALIZED))
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doShutdown] Exception occurred: {}", id, ex.toString()));
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return this.id;
  }

  @Override
  public String toString() {
    return "Microservices@" + id;
  }

  private Mono<Microservices> start() {
    return Mono.fromCallable(() -> transportBootstrap.start(this))
        .flatMap(
            transportBootstrap -> {
              final ServiceCall serviceCall = call();
              final Address serviceAddress = transportBootstrap.transportAddress;

              final ServiceEndpoint.Builder serviceEndpointBuilder =
                  ServiceEndpoint.builder()
                      .id(id)
                      .address(serviceAddress)
                      .contentTypes(DataCodec.getAllContentTypes())
                      .tags(tags);

              // invoke service providers and register services
              List<Object> serviceInstances =
                  serviceProviders.stream()
                      .flatMap(serviceProvider -> serviceProvider.provide(serviceCall).stream())
                      .peek(this::registerService)
                      .peek(
                          serviceInfo ->
                              serviceEndpointBuilder.appendServiceRegistrations(
                                  ServiceScanner.scanServiceInfo(serviceInfo)))
                      .map(ServiceInfo::serviceInstance)
                      .collect(Collectors.toList());

              serviceEndpoint = newServiceEndpoint(serviceEndpointBuilder.build());

              return concludeDiscovery(
                      this, new ServiceDiscoveryOptions().serviceEndpoint(serviceEndpoint))
                  .then(startGateway(new GatewayOptions().call(serviceCall)))
                  .then(Mono.fromCallable(() -> Injector.inject(this, serviceInstances)))
                  .then(discoveryBootstrap.startListen())
                  .thenReturn(this);
            })
        .doOnSubscribe(s -> LOGGER.info("[{}][start] Starting", id))
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .onErrorResume(ex -> Mono.defer(this::shutdown).then(Mono.error(ex)));
  }

  private ServiceEndpoint newServiceEndpoint(ServiceEndpoint serviceEndpoint) {
    ServiceEndpoint.Builder builder = ServiceEndpoint.from(serviceEndpoint);

    int port = Optional.ofNullable(externalPort).orElse(serviceEndpoint.address().port());

    // calculate local service endpoint address
    Address newAddress =
        Optional.ofNullable(externalHost)
            .map(host -> Address.create(host, port))
            .orElseGet(() -> Address.create(serviceEndpoint.address().host(), port));

    return builder.address(newAddress).build();
  }

  private Mono<GatewayBootstrap> startGateway(GatewayOptions options) {
    return Mono.fromCallable(() -> gatewayBootstrap.start(this, options));
  }

  private Mono<ServiceDiscoveryBootstrap> concludeDiscovery(
      Microservices microservices, ServiceDiscoveryOptions options) {
    return Mono.fromCallable(() -> discoveryBootstrap.conclude(microservices, options));
  }

  private void registerService(ServiceInfo serviceInfo) {
    serviceRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(defaultErrorMapper)
            .dataDecoderIfAbsent(defaultDataDecoder)
            .authenticatorIfAbsent(defaultAuthenticator)
            .principalMapperIfAbsent(defaultPrincipalMapper)
            .build());
  }

  public Address serviceAddress() {
    return transportBootstrap.transportAddress;
  }

  public ServiceCall call() {
    return new ServiceCall()
        .transport(transportBootstrap.clientTransport)
        .serviceRegistry(serviceRegistry)
        .contentType(defaultContentType)
        .errorMapper(DefaultErrorMapper.INSTANCE)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  public List<Gateway> gateways() {
    return gatewayBootstrap.gateways();
  }

  public Gateway gateway(String id) {
    return gatewayBootstrap.gateway(id);
  }

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public List<ServiceEndpoint> serviceEndpoints() {
    return serviceRegistry.listServiceEndpoints();
  }

  public Map<String, String> tags() {
    return tags;
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }

  public Address discoveryAddress() {
    return discoveryBootstrap.serviceDiscovery != null
        ? discoveryBootstrap.serviceDiscovery.address()
        : null;
  }

  /**
   * Function to subscribe and listen on the stream of {@code ServiceDiscoveryEvent}\s from
   * composite service discovery instance.
   *
   * <p>Can be called before or after composite service discovery {@code .start()} method call (i.e
   * before of after all service discovery instances will be started). If it's called before then
   * new events will be streamed from all service discovery instances, if it's called after then
   * {@link ServiceRegistry#listServiceEndpoints()} will be turned to service discovery events of
   * type {@link Type#ENDPOINT_ADDED}, and concateneted with a stream of live events.
   *
   * @return stream of {@code ServiceDiscoveryEvent}\s
   */
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return discoveryBootstrap.listen();
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> {
          shutdown.emitEmpty(RETRY_NON_SERIALIZED);
          return onShutdown.asMono();
        });
  }

  /**
   * Returns signal of when shutdown was completed.
   *
   * @return signal of when shutdown completed
   */
  public Mono<Void> onShutdown() {
    return onShutdown.asMono();
  }

  private Mono<Void> doShutdown() {
    return Mono.whenDelayError(
            applyBeforeDestroy(),
            Mono.fromRunnable(discoveryBootstrap::close),
            Mono.fromRunnable(gatewayBootstrap::close),
            Mono.fromRunnable(transportBootstrap::close))
        .doOnSubscribe(s -> LOGGER.info("[{}][doShutdown] Shutting down", id))
        .doOnSuccess(s -> LOGGER.info("[{}][doShutdown] Shutdown", id));
  }

  private Mono<Void> applyBeforeDestroy() {
    return Mono.defer(
        () ->
            Mono.whenDelayError(
                serviceRegistry.listServices().stream()
                    .map(ServiceInfo::serviceInstance)
                    .map(s -> Mono.fromRunnable(() -> Injector.processBeforeDestroy(this, s)))
                    .collect(Collectors.toList())));
  }

  @Override
  public void close() {
    try {
      shutdown().toFuture().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final class Builder {

    private Map<String, String> tags = new HashMap<>();
    private final List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private Authenticator<Object> defaultAuthenticator = null;
    private final ServiceDiscoveryBootstrap discoveryBootstrap = new ServiceDiscoveryBootstrap();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private final GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper defaultErrorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder defaultDataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);
    private String defaultContentType = ServiceMessage.DEFAULT_DATA_FORMAT;
    private PrincipalMapper<Object, Object> defaultPrincipalMapper = null;
    private String externalHost;
    private Integer externalPort;

    public Mono<Microservices> start() {
      return Mono.defer(() -> new Microservices(this).start());
    }

    public Microservices startAwait() {
      return start().block();
    }

    public Builder services(ServiceInfo... services) {
      serviceProviders.add(call -> Arrays.stream(services).collect(Collectors.toList()));
      return this;
    }

    /**
     * Adds service instance to microservice.
     *
     * @param services service instance.
     * @return builder
     */
    public Builder services(Object... services) {
      serviceProviders.add(
          call ->
              Arrays.stream(services)
                  .map(
                      s ->
                          s instanceof ServiceInfo
                              ? (ServiceInfo) s
                              : ServiceInfo.fromServiceInstance(s).build())
                  .collect(Collectors.toList()));
      return this;
    }

    public Builder services(ServiceProvider serviceProvider) {
      serviceProviders.add(serviceProvider);
      return this;
    }

    public Builder externalHost(String externalHost) {
      this.externalHost = externalHost;
      return this;
    }

    public Builder externalPort(Integer externalPort) {
      this.externalPort = externalPort;
      return this;
    }

    public Builder serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public Builder discovery(ServiceDiscoveryFactory discoveryFactory) {
      this.discoveryBootstrap.operator(options -> options.discoveryFactory(discoveryFactory));
      return this;
    }

    public Builder transport(Supplier<ServiceTransport> supplier) {
      this.transportBootstrap = new ServiceTransportBootstrap(supplier);
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder gateway(Function<GatewayOptions, Gateway> factory) {
      gatewayBootstrap.addFactory(factory);
      return this;
    }

    /**
     * Setter for default {@code errorMapper}. By default, default {@code errorMapper} is set to
     * {@link DefaultErrorMapper#INSTANCE}.
     *
     * @param errorMapper error mapper; not null
     * @return this builder with applied parameter
     */
    public Builder defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
      this.defaultErrorMapper = Objects.requireNonNull(errorMapper, "default errorMapper");
      return this;
    }

    /**
     * Setter for default {@code dataDecoder}. By default, default {@code dataDecoder} is set to
     * {@link ServiceMessageDataDecoder#INSTANCE} if it exists, otherswise to a function {@code
     * (message, dataType) -> message}
     *
     * @param dataDecoder data decoder; not null
     * @return this builder with applied parameter
     */
    public Builder defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.defaultDataDecoder = Objects.requireNonNull(dataDecoder, "default dataDecoder");
      return this;
    }

    /**
     * Setter for default {@code contentType}. By default, default {@code contentType} is set to
     * {@link ServiceMessage#DEFAULT_DATA_FORMAT}.
     *
     * @param contentType contentType; not null
     * @return this builder with applied parameter
     */
    public Builder defaultContentType(String contentType) {
      this.defaultContentType = Objects.requireNonNull(contentType, "default contentType");
      return this;
    }

    /**
     * Setter for default {@code authenticator}. By default, default {@code authenticator} is null.
     *
     * @param authenticator authenticator; optional
     * @return this builder with applied parameter
     */
    public <T> Builder defaultAuthenticator(Authenticator<? extends T> authenticator) {
      //noinspection unchecked
      this.defaultAuthenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    /**
     * Setter for default {@code principalMapper}. By default, default {@code principalMapper} is
     * null.
     *
     * @param principalMapper principalMapper; optional
     * @param <T> auth data type
     * @param <R> principal type
     * @return this builder with applied parameter
     */
    public <T, R> Builder defaultPrincipalMapper(
        PrincipalMapper<? super T, ? extends R> principalMapper) {
      //noinspection unchecked
      this.defaultPrincipalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }
  }

  private static class ServiceDiscoveryBootstrap implements AutoCloseable {

    private UnaryOperator<ServiceDiscoveryOptions> operator;
    private ServiceDiscovery serviceDiscovery;

    // Sink
    private final Sinks.Many<ServiceDiscoveryEvent> sink =
        Sinks.many().multicast().directBestEffort();

    private final Disposable.Composite disposables = Disposables.composite();
    private Scheduler scheduler;
    private Microservices microservices;

    private ServiceDiscoveryBootstrap operator(UnaryOperator<ServiceDiscoveryOptions> op) {
      operator = op;
      return this;
    }

    private ServiceDiscoveryBootstrap conclude(
        Microservices microservices, ServiceDiscoveryOptions options) {
      if (operator == null) {
        return this;
      }

      options = operator.apply(options);
      final ServiceEndpoint serviceEndpoint = options.serviceEndpoint();
      final ServiceDiscoveryFactory discoveryFactory = options.discoveryFactory();

      if (discoveryFactory == null) {
        return this;
      }

      serviceDiscovery = discoveryFactory.createServiceDiscovery(serviceEndpoint);
      this.microservices = microservices;
      this.scheduler = Schedulers.newSingle("discovery", true);

      return this;
    }

    private Mono<Void> startListen() {
      return Mono.defer(
          () -> {
            if (serviceDiscovery == null) {
              return Mono.empty();
            }

            disposables.add(
                serviceDiscovery
                    .listen()
                    .subscribeOn(scheduler)
                    .publishOn(scheduler)
                    .doOnNext(event -> onDiscoveryEvent(microservices, event))
                    .doOnNext(event -> sink.emitNext(event, RETRY_NON_SERIALIZED))
                    .subscribe());

            return Mono.fromRunnable(serviceDiscovery::start)
                .then()
                .doOnSubscribe(s -> LOGGER.info("[{}][startListen] Starting", microservices.id()))
                .doOnSuccess(avoid -> LOGGER.info("[{}][startListen] Started", microservices.id()))
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "[{}][startListen] Exception occurred: {}",
                            microservices.id(),
                            ex.toString()));
          });
    }

    public Flux<ServiceDiscoveryEvent> listen() {
      return Flux.fromStream(microservices.serviceRegistry.listServiceEndpoints().stream())
          .map(ServiceDiscoveryEvent::newEndpointAdded)
          .concatWith(sink.asFlux().onBackpressureBuffer())
          .subscribeOn(scheduler)
          .publishOn(scheduler);
    }

    private void onDiscoveryEvent(Microservices microservices, ServiceDiscoveryEvent event) {
      if (event.isEndpointAdded()) {
        microservices.serviceRegistry.registerService(event.serviceEndpoint());
      }
      if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
        microservices.serviceRegistry.unregisterService(event.serviceEndpoint().id());
      }
    }

    @Override
    public void close() {
      disposables.dispose();

      sink.emitComplete(RETRY_NON_SERIALIZED);

      try {
        if (serviceDiscovery != null) {
          serviceDiscovery.shutdown();
        }
      } finally {
        if (scheduler != null) {
          scheduler.dispose();
        }
      }
    }
  }

  private static class GatewayBootstrap implements AutoCloseable {

    private final List<Function<GatewayOptions, Gateway>> factories = new ArrayList<>();
    private final List<Gateway> gateways = new CopyOnWriteArrayList<>();

    private GatewayBootstrap addFactory(Function<GatewayOptions, Gateway> factory) {
      this.factories.add(factory);
      return this;
    }

    private GatewayBootstrap start(Microservices microservices, GatewayOptions options) {
      for (Function<GatewayOptions, Gateway> factory : factories) {
        LOGGER.info("[{}][gateway][{}][start] Starting", microservices.id(), options.id());

        try {
          final Gateway gateway = factory.apply(options).start().toFuture().get();

          gateways.add(gateway);

          LOGGER.info(
              "[{}][gateway][{}][start] Started, address: {}",
              microservices.id(),
              gateway.id(),
              gateway.address());
        } catch (Exception ex) {
          LOGGER.error(
              "[{}][gateway][{}][start] Exception occurred: {}",
              microservices.id(),
              options.id(),
              ex.toString());
          throw new RuntimeException(ex);
        }
      }

      return this;
    }

    private List<Gateway> gateways() {
      return new ArrayList<>(gateways);
    }

    private Gateway gateway(String id) {
      return gateways.stream()
          .filter(gateway -> gateway.id().equals(id))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Cannot find gateway by id=" + id));
    }

    @Override
    public void close() {
      try {
        Mono.whenDelayError(gateways.stream().map(Gateway::stop).toArray(Mono[]::new))
            .toFuture()
            .get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ServiceTransportBootstrap implements AutoCloseable {

    private final Supplier<ServiceTransport> transportSupplier;

    private ServiceTransport serviceTransport;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address transportAddress = Address.NULL_ADDRESS;

    private ServiceTransportBootstrap() {
      this(null);
    }

    private ServiceTransportBootstrap(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
    }

    private ServiceTransportBootstrap start(Microservices microservices) {
      if (transportSupplier == null || (serviceTransport = transportSupplier.get()) == null) {
        return this;
      }

      LOGGER.info("[{}][serviceTransport][start] Starting", microservices.id());

      try {
        try {
          serviceTransport = serviceTransport.start();
          serverTransport = serviceTransport.serverTransport(microservices.serviceRegistry).bind();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        transportAddress = prepareAddress(serverTransport.address());
        clientTransport = serviceTransport.clientTransport();

        LOGGER.info(
            "[{}][serviceTransport][start] Started, address: {}",
            microservices.id(),
            serverTransport.address());

        return this;
      } catch (Exception ex) {
        LOGGER.error(
            "[{}][serviceTransport][start] Exception occurred: {}",
            microservices.id(),
            ex.toString());
        throw new RuntimeException(ex);
      }
    }

    private static Address prepareAddress(Address address) {
      final InetAddress inetAddress;
      try {
        inetAddress = InetAddress.getByName(address.host());
      } catch (UnknownHostException e) {
        throw Exceptions.propagate(e);
      }
      if (inetAddress.isAnyLocalAddress()) {
        return Address.create(Address.getLocalIpAddress().getHostAddress(), address.port());
      } else {
        return Address.create(inetAddress.getHostAddress(), address.port());
      }
    }

    @Override
    public void close() {
      if (serverTransport != null) {
        try {
          serverTransport.stop();
        } catch (Exception e) {
          LOGGER.warn("[serverTransport][stop] Exception: {}", e.toString());
        }
      }

      if (serviceTransport != null) {
        try {
          serviceTransport.stop();
        } catch (Exception e) {
          LOGGER.warn("[serviceTransport][stop] Exception: {}", e.toString());
        }
      }
    }
  }
}
