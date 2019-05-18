package io.scalecube.services.gateway;

import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientSettings;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.transport.api.Address;
import java.util.function.Function;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLocalGatewayExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalGatewayExtension.class);

  private final Microservices gateway;

  private Client client;
  private Address gatewayAddress;

  protected AbstractLocalGatewayExtension(
      Object serviceInstance, Function<GatewayOptions, Gateway> gatewayFactory) {

    gateway =
        Microservices.builder().services(serviceInstance).gateway(gatewayFactory).startAwait();
  }

  @Override
  public final void afterAll(ExtensionContext context) {
    if (gateway != null) {
      try {
        gateway.shutdown().block();
      } catch (Throwable ignore) {
        // ignore
      }
      LOGGER.info("Shutdown gateway {}", gateway);
    }
  }

  @Override
  public final void afterEach(ExtensionContext context) {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public final void beforeEach(ExtensionContext context) {
    client = new Client(transport(), clientMessageCodec());
  }

  public Client client() {
    return client;
  }

  @Override
  public final void beforeAll(ExtensionContext context) {
    gatewayAddress = gateway.gateway(gatewayAliasName()).address();
  }

  protected final ClientSettings clientSettings() {
    return ClientSettings.builder().host(gatewayAddress.host()).port(gatewayAddress.port()).build();
  }

  protected abstract ClientTransport transport();

  protected abstract ClientCodec clientMessageCodec();

  protected abstract String gatewayAliasName();
}
