package io.scalecube.services.gateway;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.transport.GatewayClientSettings;
import io.scalecube.services.gateway.transport.StaticAddressRouter;
import io.scalecube.services.transport.api.ClientTransport;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.resources.LoopResources;

public abstract class AbstractLocalGatewayExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalGatewayExtension.class);

  private final ServiceInfo serviceInfo;
  private final Function<GatewayOptions, Gateway> gatewaySupplier;
  private final Function<GatewayClientSettings, ClientTransport> clientSupplier;

  private Microservices gateway;
  private LoopResources clientLoopResources;
  private ServiceCall clientServiceCall;
  private String gatewayId;

  protected AbstractLocalGatewayExtension(
      ServiceInfo serviceInfo,
      Function<GatewayOptions, Gateway> gatewaySupplier,
      Function<GatewayClientSettings, ClientTransport> clientSupplier) {
    this.serviceInfo = serviceInfo;
    this.gatewaySupplier = gatewaySupplier;
    this.clientSupplier = clientSupplier;
  }

  @Override
  public final void beforeAll(ExtensionContext context) {

    gateway =
        Microservices.builder()
            .services(serviceInfo)
            .gateway(
                options -> {
                  Gateway gateway = gatewaySupplier.apply(options);
                  gatewayId = gateway.id();
                  return gateway;
                })
            .startAwait();

    clientLoopResources = LoopResources.create("gateway-client-transport-worker");
  }

  @Override
  public final void beforeEach(ExtensionContext context) {
    Address address = gateway.gateway(gatewayId).address();

    GatewayClientSettings settings = GatewayClientSettings.builder().address(address).build();

    clientServiceCall =
        new ServiceCall()
            .transport(clientSupplier.apply(settings))
            .router(new StaticAddressRouter(address));
  }

  @Override
  public final void afterAll(ExtensionContext context) {
    Optional.ofNullable(clientLoopResources).ifPresent(LoopResources::dispose);
    shutdownGateway();
  }

  public ServiceCall client() {
    return clientServiceCall;
  }

  private void shutdownGateway() {
    if (gateway != null) {
      try {
        gateway.shutdown().block();
      } catch (Throwable ignore) {
        // ignore
      }
      LOGGER.info("Shutdown gateway {}", gateway);
    }
  }
}
