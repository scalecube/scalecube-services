package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.rsocket.websocket.RSocketWebSocketGateway;
import io.scalecube.services.Microservices;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RSocketGatewayResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketGatewayResource.class);


  private RSocketWebSocketGateway gateway;
  private RSocketGatewayClient client;

  public RSocketWebSocketGateway startGateway(Microservices microservices) {
    gateway = new RSocketWebSocketGateway(microservices);
    InetSocketAddress address = gateway.start();
    client = new RSocketGatewayClient(address);
    return gateway;
  }

  public void stopGateway() {
    if (gateway != null) {
      try {
        gateway.stop();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Stopped websocket server {} on {}", gateway, gateway.address());
    }
  }

  public RSocketWebSocketGateway gateway() {
    return gateway;
  }

  public RSocketGatewayClient client() {
    return client;
  }

  @Override
  protected void after() {
    stopGateway();
  }

}
