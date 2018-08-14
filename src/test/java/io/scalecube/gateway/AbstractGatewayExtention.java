package io.scalecube.gateway;

import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.Microservices;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.transport.Address;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public abstract class AbstractGatewayExtention
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGatewayExtention.class);

  protected final Microservices seed;

  private Client client;
  private InetSocketAddress gatewayAddress;
  private final Object serviceInstance;
  private Microservices services;

  public AbstractGatewayExtention(Object serviceInstance) {
    this.seed = Microservices.builder().startAwait();
    this.serviceInstance = serviceInstance;
  }

  @Override
  public final void beforeAll(ExtensionContext context) {
    gatewayAddress = startGateway();
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
    initClient();
  }

  public Client client() {
    return client;
  }

  public void startServices() {
    this.services = Microservices.builder()
        .seeds(seed.discovery().address())
        .services(serviceInstance)
        .startAwait();
    Address serviceAddress = services.serviceAddress();
    LOGGER.info("Started services {} on {}", services, serviceAddress);
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

  public abstract InetSocketAddress startGateway();

  public abstract void shutdownGateway();

  protected abstract RSocketClientTransport transport(ClientSettings settings, ClientMessageCodec codec);

  private Client initClient() {
    ClientSettings settings = ClientSettings.builder()
        .host(gatewayAddress.getHostName())
        .port(gatewayAddress.getPort())
        .build();

    ClientMessageCodec codec = new ClientMessageCodec(
        HeadersCodec.getInstance(settings.contentType()),
        DataCodec.getInstance(settings.contentType()));

    return new Client(transport(settings, codec), codec);
  }
}
