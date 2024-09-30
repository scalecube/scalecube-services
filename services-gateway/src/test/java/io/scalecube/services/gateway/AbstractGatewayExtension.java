//package io.scalecube.services.gateway;
//
//import io.scalecube.services.Address;
//import io.scalecube.services.Microservices;
//import io.scalecube.services.ServiceCall;
//import io.scalecube.services.ServiceEndpoint;
//import io.scalecube.services.ServiceInfo;
//import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
//import io.scalecube.services.discovery.api.ServiceDiscovery;
//import io.scalecube.services.gateway.client.GatewayClientSettings;
//import io.scalecube.services.gateway.client.StaticAddressRouter;
//import io.scalecube.services.transport.api.ClientTransport;
//import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
//import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
//import java.util.function.Function;
//import org.junit.jupiter.api.extension.AfterAllCallback;
//import org.junit.jupiter.api.extension.AfterEachCallback;
//import org.junit.jupiter.api.extension.BeforeAllCallback;
//import org.junit.jupiter.api.extension.BeforeEachCallback;
//import org.junit.jupiter.api.extension.ExtensionContext;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public abstract class AbstractGatewayExtension
//    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGatewayExtension.class);
//
//  private final ServiceInfo serviceInfo;
//  private final Function<GatewayOptions, Gateway> gatewaySupplier;
//  private final Function<GatewayClientSettings, ClientTransport> clientSupplier;
//
//  private String gatewayId;
//  private Microservices gateway;
//  private Microservices services;
//  private ServiceCall clientServiceCall;
//
//  protected AbstractGatewayExtension(
//      ServiceInfo serviceInfo,
//      Function<GatewayOptions, Gateway> gatewaySupplier,
//      Function<GatewayClientSettings, ClientTransport> clientSupplier) {
//    this.serviceInfo = serviceInfo;
//    this.gatewaySupplier = gatewaySupplier;
//    this.clientSupplier = clientSupplier;
//  }
//
//  @Override
//  public final void beforeAll(ExtensionContext context) {
//    gateway =
//        Microservices.builder()
//            .discovery(
//                serviceEndpoint ->
//                    new ScalecubeServiceDiscovery()
//                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
//                        .options(opts -> opts.metadata(serviceEndpoint)))
//            .transport(RSocketServiceTransport::new)
//            .gateway(
//                options -> {
//                  Gateway gateway = gatewaySupplier.apply(options);
//                  gatewayId = gateway.id();
//                  return gateway;
//                })
//            .startAwait();
//    startServices();
//  }
//
//  @Override
//  public final void beforeEach(ExtensionContext context) {
//    // if services was shutdown in test need to start them again
//    if (services == null) {
//      startServices();
//    }
//    Address gatewayAddress = gateway.gateway(gatewayId).address();
//    GatewayClientSettings clintSettings =
//        GatewayClientSettings.builder().address(gatewayAddress).build();
//    clientServiceCall =
//        new ServiceCall()
//            .transport(clientSupplier.apply(clintSettings))
//            .router(new StaticAddressRouter(gatewayAddress));
//  }
//
//  @Override
//  public final void afterEach(ExtensionContext context) {
//    // no-op
//  }
//
//  @Override
//  public final void afterAll(ExtensionContext context) {
//    shutdownServices();
//    shutdownGateway();
//  }
//
//  public ServiceCall client() {
//    return clientServiceCall;
//  }
//
//  public void startServices() {
//    services =
//        Microservices.builder()
//            .discovery(this::serviceDiscovery)
//            .transport(RSocketServiceTransport::new)
//            .services(serviceInfo)
//            .startAwait();
//    LOGGER.info("Started services {} on {}", services, services.serviceAddress());
//  }
//
//  private ServiceDiscovery serviceDiscovery(ServiceEndpoint serviceEndpoint) {
//    return new ScalecubeServiceDiscovery()
//        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
//        .options(opts -> opts.metadata(serviceEndpoint))
//        .membership(opts -> opts.seedMembers(gateway.discoveryAddress().toString()));
//  }
//
//  public void shutdownServices() {
//    if (services != null) {
//      try {
//        services.shutdown().block();
//      } catch (Throwable ignore) {
//        // ignore
//      }
//      LOGGER.info("Shutdown services {}", services);
//
//      // if this method is called in particular test need to indicate that services are stopped to
//      // start them again before another test
//      services = null;
//    }
//  }
//
//  private void shutdownGateway() {
//    if (gateway != null) {
//      try {
//        gateway.shutdown().block();
//      } catch (Throwable ignore) {
//        // ignore
//      }
//      LOGGER.info("Shutdown gateway {}", gateway);
//    }
//  }
//}
