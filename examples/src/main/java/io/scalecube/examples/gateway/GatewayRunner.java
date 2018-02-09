package io.scalecube.examples.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIoServer;
import io.scalecube.ipc.ClientStream;
import io.scalecube.ipc.ListeningServerStream;
import io.scalecube.ipc.ServerStream;
import io.scalecube.transport.Address;

/**
 * Runner for gateway server (on http and socketio ports) to services exmaple.
 */
public final class GatewayRunner {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    ClientStream clientStream = ClientStream.newClientStream();
    ServerStream serverStream = ServerStream.newServerStream();

    serverStream.listenReadSuccess().subscribe(event -> {
      System.out.println("Sending ...");
      clientStream.send(Address.create("127.0.0.1", 5801), event.getMessage().get());
    });

    clientStream.listenReadSuccess().subscribe(event -> {
      System.out.println("Got reply: " + event + " sending it back to gateway client");
      serverStream.send(event.getMessage().get());
    });

    GatewaySocketIoServer.onPort(4040, serverStream).start();
    GatewayHttpServer.onPort(8080, serverStream).start();

    ListeningServerStream serverStream1 = ListeningServerStream.newServerStream()
        .withListenAddress("127.0.0.1").withPort(5801).bind();
    serverStream1.listenReadSuccess().subscribe(event -> serverStream1.send(event.getMessage().get()));

    Thread.currentThread().join();
  }
}
