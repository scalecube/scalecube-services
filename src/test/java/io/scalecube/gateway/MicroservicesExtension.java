package io.scalecube.gateway;

import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.transport.Address;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class MicroservicesExtension implements AfterAllCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(MicroservicesExtension.class);
  private static final String REPORTER_PATH = "metrics/tests";

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices services;
  private InetSocketAddress serviceAddress;

  public Microservices getGateway() {
    return gateway;
  }

  public Address getGatewayAddress() {
    return gatewayAddress;
  }

  public Microservices getServices() {
    return services;
  }

  public InetSocketAddress getServiceAddress() {
    return serviceAddress;
  }

  @SafeVarargs
  public final MicroservicesExtension startGateway(Class<? extends Gateway>... gateways) {
    Microservices.Builder builder = Microservices.builder();
    for (Class<? extends Gateway> gateway : gateways) {
      builder.gateway(GatewayConfig.builder(gateway.getSimpleName(), gateway).build());
    }
    gateway = builder.startAwait();
    gatewayAddress = gateway.discovery().address();
    LOGGER.info("Started gateway {} on {}", gateway, gatewayAddress);
    return this;
  }

  public MicroservicesExtension startServices(Address gatewayAddress) {
    return startServices(gatewayAddress, new GreetingServiceImpl());
  }

  public MicroservicesExtension startServices(Address gatewayAddress, GreetingService service) {
    services = Microservices.builder()
        .seeds(gatewayAddress)
        .services(service)
        .startAwait();
    serviceAddress = services.serviceAddress();
    LOGGER.info("Started services {} on {}", services, serviceAddress);
    return this;
  }

  public MicroservicesExtension shutdownGateway() {
    if (gateway != null) {
      try {
        gateway.shutdown().block();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown gateway {} on {}", gateway, gatewayAddress);
    }
    return this;
  }

  public MicroservicesExtension shutdownServices() {
    if (services != null) {
      try {
        services.shutdown().block();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown services {} on {}", services, serviceAddress);
    }
    return this;
  }

  @Override
  public void afterAll(ExtensionContext context) {
    shutdownGateway();
    shutdownServices();
  }
}
