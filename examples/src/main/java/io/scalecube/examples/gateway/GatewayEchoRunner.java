package io.scalecube.examples.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIoServer;
import io.scalecube.streams.Event;
import io.scalecube.streams.ServerStream;

/**
 * Runner for standalone gateway server on http and socketio ports.
 */
public final class GatewayEchoRunner {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    ServerStream serverStream = ServerStream.newServerStream();
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    GatewaySocketIoServer.onPort(4040, serverStream).start();
    GatewayHttpServer.onPort(8080, serverStream).start();

    Thread.currentThread().join();
  }
}
