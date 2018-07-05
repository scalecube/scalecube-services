package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.services.Microservices;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RSocketGatewayResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketGatewayResource.class);


  private RSocketWebsocketGateway gateway;
  private RSocketGatewayClient client;

  public RSocketWebsocketGateway startGateway(Microservices microservices) {
    gateway = new RSocketWebsocketGateway(microservices);
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

  public RSocketWebsocketGateway gateway() {
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
