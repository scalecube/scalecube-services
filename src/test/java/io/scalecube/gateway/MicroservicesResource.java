package io.scalecube.gateway;

import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroservicesResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MicroservicesResource.class);

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices services;
  private Address serviceAddress;

  public Microservices getGateway() {
    return gateway;
  }

  public Address getGatewayAddress() {
    return gatewayAddress;
  }

  public Microservices getServices() {
    return services;
  }

  public Address getServiceAddress() {
    return serviceAddress;
  }

  public MicroservicesResource startGateway() {
    gateway = Microservices.builder().startAwait();
    gatewayAddress = gateway.cluster().address();
    LOGGER.info("Started gateway {} on {}", gateway, gatewayAddress);
    return this;
  }

  public MicroservicesResource startServices(Address gatewayAddress) {
    return startServices(gatewayAddress, new GreetingServiceImpl());
  }

  public MicroservicesResource startServices(Address gatewayAddress, GreetingService service) {
    services = Microservices.builder()
        .seeds(gatewayAddress)
        .services(service)
        .startAwait();
    serviceAddress = services.serviceAddress();
    LOGGER.info("Started services {} on {}", services, serviceAddress);
    return this;
  }

  public MicroservicesResource shutdownGateway() {
    if (gateway != null) {
      try {
        gateway.shutdown();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown gateway {} on {}", gateway, gatewayAddress);
    }
    return this;
  }

  public MicroservicesResource shutdownServices() {
    if (services != null) {
      try {
        services.shutdown();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Shutdown services {} on {}", services, serviceAddress);
    }
    return this;
  }

  @Override
  protected void after() {
    shutdownGateway();
    shutdownServices();
  }
}
