package io.scalecube.services;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
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
public class Microservices implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final Microservices.Context context;
  private final String id = UUID.randomUUID().toString();

  private ServiceTransport serviceTransport;
  private ClientTransport clientTransport;
  private ServerTransport serverTransport;
  private Address serviceAddress = Address.NULL_ADDRESS;

  private ServiceEndpoint serviceEndpoint;
  private ServiceCall serviceCall;
  private List<Object> serviceInstances;
  private final List<Gateway> gateways = new ArrayList<>();
  private ServiceDiscovery serviceDiscovery;
  private final Sinks.Many<ServiceDiscoveryEvent> sink =
      Sinks.many().multicast().directBestEffort();
  private final Disposable.Composite disposables = Disposables.composite();
  private Scheduler scheduler;

  private Microservices(Microservices.Context context) {
    this.context = context;
  }

  public static Microservices start(Microservices.Context context) {
    final Microservices microservices = new Microservices(context.conclude());
    try {
      microservices.startTransport(context.transportSupplier, context.serviceRegistry);
      microservices.buildServiceEndpoint();
      microservices.startGateways();
    } catch (Exception ex) {
      microservices.close();
      throw Exceptions.propagate(ex);
    }
    return microservices;
  }

  private void startTransport(
      Supplier<ServiceTransport> transportSupplier, ServiceRegistry serviceRegistry) {
    if (transportSupplier == null) {
      return;
    }
    serviceTransport = transportSupplier.get().start();
    serverTransport = serviceTransport.serverTransport(serviceRegistry).bind();
    clientTransport = serviceTransport.clientTransport();
    serviceAddress = prepareAddress(serverTransport.address());
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

  private Mono<Microservices> start() {
    return Mono.fromCallable(() -> transportBootstrap.start(this))
        .flatMap(
            transportBootstrap -> {
              return concludeDiscovery(
                      this, new ServiceDiscoveryOptions().serviceEndpoint(serviceEndpoint))
                  .then(startGateways(new GatewayOptions().call(serviceCall)))
                  .then(Mono.fromCallable(() -> Injector.inject(this, serviceInstances)))
                  .then(discoveryBootstrap.startListen())
                  .thenReturn(this);
            })
        .doOnSubscribe(s -> LOGGER.info("[{}][start] Starting", id))
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .onErrorResume(ex -> Mono.defer(this::shutdown).then(Mono.error(ex)));
  }

  private void buildServiceEndpoint() {
    serviceCall = call();

    final ServiceEndpoint.Builder builder =
        ServiceEndpoint.builder()
            .id(id)
            .address(serviceAddress)
            .contentTypes(DataCodec.getAllContentTypes())
            .tags(context.tags);

    serviceInstances =
        context.serviceProviders.stream()
            .flatMap(serviceProvider -> serviceProvider.provide(serviceCall).stream())
            .peek(this::registerService)
            .peek(
                serviceInfo ->
                    builder.appendServiceRegistrations(ServiceScanner.scanServiceInfo(serviceInfo)))
            .map(ServiceInfo::serviceInstance)
            .collect(Collectors.toList());

    serviceEndpoint = newServiceEndpoint(builder.build());
  }

  private ServiceEndpoint newServiceEndpoint(ServiceEndpoint serviceEndpoint) {
    final ServiceEndpoint.Builder builder = ServiceEndpoint.from(serviceEndpoint);

    final String finalHost =
        Optional.ofNullable(context.externalHost).orElse(serviceEndpoint.address().host());
    final int finalPort =
        Optional.ofNullable(context.externalPort).orElse(serviceEndpoint.address().port());

    return builder.address(Address.create(finalHost, finalPort)).build();
  }

  private void startGateways() {
    final GatewayOptions options = new GatewayOptions().call(serviceCall);
    for (Function<GatewayOptions, Gateway> factory : context.gatewayFactories) {
      gateways.add(factory.apply(options).start());
    }
  }

  private void registerService(ServiceInfo serviceInfo) {
    context.serviceRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(context.defaultErrorMapper)
            .dataDecoderIfAbsent(context.defaultDataDecoder)
            .authenticatorIfAbsent(context.defaultAuthenticator)
            .principalMapperIfAbsent(context.defaultPrincipalMapper)
            .build());
  }

  public Address serviceAddress() {
    return serviceAddress;
  }

  public ServiceCall call() {
    return new ServiceCall()
        .transport(clientTransport)
        .serviceRegistry(context.serviceRegistry)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  public List<Gateway> gateways() {
    return gateways;
  }

  public Gateway gateway(String id) {
    return gateways.stream()
        .filter(gateway -> gateway.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Cannot find gateway by id=" + id));
  }

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public List<ServiceEndpoint> serviceEndpoints() {
    return context.serviceRegistry.listServiceEndpoints();
  }

  public Map<String, String> tags() {
    return context.tags;
  }

  public ServiceRegistry serviceRegistry() {
    return context.serviceRegistry;
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
    if (clientTransport != null) {
      try {
        clientTransport.close();
      } catch (Exception e) {
        // TODO: log it
      }
    }

    if (serverTransport != null) {
      try {
        serverTransport.stop();
      } catch (Exception e) {
        // TODO: log it
      }
    }

    if (serviceTransport != null) {
      try {
        serviceTransport.stop();
      } catch (Exception e) {
        // TODO: log it
      }
    }

    for (Gateway gateway : gateways) {
      try {
        gateway.stop();
      } catch (Exception ex) {
        // TODO: log it
      }
    }
  }

  public static final class Context {

    private final AtomicBoolean isConcluded = new AtomicBoolean();

    private Map<String, String> tags = new HashMap<>();
    private final List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private Authenticator<Object> defaultAuthenticator = null;
    private PrincipalMapper<Object, Object> defaultPrincipalMapper = null;
    private ServiceProviderErrorMapper defaultErrorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder defaultDataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);
    private String externalHost;
    private Integer externalPort;
    private ServiceDiscoveryFactory discoveryFactory;
    private Supplier<ServiceTransport> transportSupplier;
    private final List<Function<GatewayOptions, Gateway>> gatewayFactories = new ArrayList<>();

    public Context() {}

    public Context services(ServiceInfo... services) {
      serviceProviders.add(call -> Arrays.stream(services).collect(Collectors.toList()));
      return this;
    }

    public Context services(Object... services) {
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

    public Context services(ServiceProvider serviceProvider) {
      serviceProviders.add(serviceProvider);
      return this;
    }

    public Context externalHost(String externalHost) {
      this.externalHost = externalHost;
      return this;
    }

    public Context externalPort(Integer externalPort) {
      this.externalPort = externalPort;
      return this;
    }

    public Context tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Context serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public Context discovery(ServiceDiscoveryFactory discoveryFactory) {
      this.discoveryFactory = discoveryFactory;
      return this;
    }

    public Context transport(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
      return this;
    }

    public Context gateway(Function<GatewayOptions, Gateway> factory) {
      gatewayFactories.add(factory);
      return this;
    }

    /**
     * Setter for default {@code errorMapper}. By default, default {@code errorMapper} is set to
     * {@link DefaultErrorMapper#INSTANCE}.
     *
     * @param errorMapper error mapper; not null
     * @return this builder with applied parameter
     */
    public Context defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
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
    public Context defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.defaultDataDecoder = Objects.requireNonNull(dataDecoder, "default dataDecoder");
      return this;
    }

    /**
     * Setter for default {@code authenticator}. By default, default {@code authenticator} is null.
     *
     * @param authenticator authenticator; optional
     * @return this builder with applied parameter
     */
    public <T> Context defaultAuthenticator(Authenticator<? extends T> authenticator) {
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
    public <T, R> Context defaultPrincipalMapper(
        PrincipalMapper<? super T, ? extends R> principalMapper) {
      //noinspection unchecked
      this.defaultPrincipalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }

    private Context conclude() {
      if (isConcluded.compareAndSet(false, true)) {
        throw new IllegalStateException("Context is already concluded");
      }

      // TODO: add validations

      return this;
    }
  }
}
