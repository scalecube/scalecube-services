package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;
import io.scalecube.services.Microservices;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RSocketWebsocketResource extends ExternalResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketWebsocketResource.class);

  private RSocketWebsocketServer gateway;
  private RSocketWebsocketClient client;

  public RSocketWebsocketServer startGateway(Microservices microservices) {
    gateway = new RSocketWebsocketServer(microservices);
    InetSocketAddress address = gateway.start();
    client = new RSocketWebsocketClient(address);
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

  public RSocketWebsocketServer gateway() {
    return gateway;
  }

  public RSocketWebsocketClient client() {
    return client;
  }

  @Override
  protected void after() {
    stopGateway();
  }

}
