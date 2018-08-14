package io.scalecube.gateway.rsocket;

import io.scalecube.gateway.AbstractGatewayExtention;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;

import reactor.ipc.netty.resources.LoopResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RsocketGatewayExtension extends AbstractGatewayExtention {
  private static final Logger LOGGER = LoggerFactory.getLogger(RsocketGatewayExtension.class);

  private RSocketWebsocketServer gateway;

  public RsocketGatewayExtension(Object serviceInstance) {
    super(serviceInstance);
  }

  @Override
  public InetSocketAddress startGateway() {
    gateway = new RSocketWebsocketServer(seed);
    return gateway.start();
  }

  @Override
  public void shutdownGateway() {
    if (gateway != null) {
      try {
        gateway.stop();
      } catch (Throwable ignore) {
      }
      LOGGER.info("Stopped websocket server {} on {}", gateway, gateway.address());
    }
  }

  @Override
  protected RSocketClientTransport transport(ClientSettings settings, ClientMessageCodec codec) {
    return new RSocketClientTransport(
        settings,
        codec,
        LoopResources.create("eventLoop"));
  }
}
