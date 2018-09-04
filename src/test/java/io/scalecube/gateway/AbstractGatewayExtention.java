package io.scalecube.gateway;

import io.rsocket.Payload;
import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.codec.RSocketPayloadCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.Microservices;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.gateway.GatewayConfig;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGatewayExtention
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGatewayExtention.class);

  protected final Microservices seed;

  private final Object serviceInstance;
  private final GatewayConfig gatewayConfig;

  private Client client;
  private InetSocketAddress gatewayAddress;
  private Microservices services;

  public AbstractGatewayExtention(Object serviceInstance, GatewayConfig gatewayConfig) {
    this.gatewayConfig = gatewayConfig;
    this.serviceInstance = serviceInstance;
    this.seed = Microservices.builder().gateway(gatewayConfig).startAwait();
  }

  @Override
  public final void beforeAll(ExtensionContext context) {
    gatewayAddress = seed.gatewayAddress(gatewayAliasName(), gatewayConfig.gatewayClass());
    startServices();
  }

  @Override
  public final void afterAll(ExtensionContext context) {
    shutdownServices();
    shutdownGateway();
  }

  @Override
  public final void afterEach(ExtensionContext context) {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public final void beforeEach(ExtensionContext context) {
    client = initClient();
  }

  public Client client() {
    return client;
  }

  public void startServices() {
    this.services =
        Microservices.builder()
            .seeds(seed.discovery().address())
            .services(serviceInstance)
            .startAwait();
    LOGGER.info("Started services {} on {}", services, services.serviceAddress());
  }

  public void shutdownServices() {
    if (services != null) {
      try {
        services.shutdown().block();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown services {}", services);
    }
  }

  public void shutdownGateway() {
    if (seed != null) {
      try {
        seed.shutdown().block();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown gateway {}", seed);
    }
  }

  protected abstract RSocketClientTransport transport(
      ClientSettings settings, ClientMessageCodec<Payload> codec);

  protected abstract String gatewayAliasName();

  private Client initClient() {
    ClientSettings settings =
        ClientSettings.builder()
            .host(gatewayAddress.getHostName())
            .port(gatewayAddress.getPort())
            .build();

    ClientMessageCodec<Payload> codec =
        new RSocketPayloadCodec(
            HeadersCodec.getInstance(settings.contentType()),
            DataCodec.getInstance(settings.contentType()));

    return new Client(transport(settings, codec), codec);
  }
}
