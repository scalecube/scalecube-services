package io.scalecube.examples.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIoServer;
import io.scalecube.ipc.ServerStream;

/**
 * Runner for standalone gateway server on http and socketio ports.
 */
public final class GatewayEchoRunner {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    ServerStream serverStream = ServerStream.newServerStream();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));

    GatewaySocketIoServer.onPort(4040, serverStream).start();
    GatewayHttpServer.onPort(8080, serverStream).start();

    Thread.currentThread().join();
  }
}
