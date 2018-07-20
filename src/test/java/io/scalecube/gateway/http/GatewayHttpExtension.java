package io.scalecube.gateway.http;

import io.scalecube.services.Microservices;

import reactor.ipc.netty.http.client.HttpClient;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class GatewayHttpExtension implements AfterAllCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpExtension.class);

  private GatewayHttpServer gateway;
  private HttpClient client;

  public GatewayHttpServer startGateway(Microservices microservices) {
    gateway = new GatewayHttpServer(microservices);
    InetSocketAddress address = gateway.start();
    client = HttpClient.create(address.getPort());
    return gateway;
  }

  public void stopGateway() {
    if (gateway != null) {
      try {
        gateway.stop();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Stopped http server {} on {}", gateway, gateway.address());
    }
  }

  public GatewayHttpServer gateway() {
    return gateway;
  }

  public HttpClient client() {
    return client;
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    stopGateway();
  }
}
