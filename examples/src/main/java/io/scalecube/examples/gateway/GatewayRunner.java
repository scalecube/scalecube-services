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

  public static final Address SERVICE_ADDRESS = Address.create("127.0.1.1", 5801);

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    ClientStream clientStream = ClientStream.newClientStream();
    ServerStream serverStream = ServerStream.newServerStream();

    serverStream.listenMessageReadSuccess().subscribe(message -> clientStream.send(SERVICE_ADDRESS, message));
    clientStream.listenMessageReadSuccess().subscribe(serverStream::send);

    GatewaySocketIoServer.onPort(4040, serverStream).start();
    GatewayHttpServer.onPort(8080, serverStream).start();

    ListeningServerStream serverStream1 = ListeningServerStream.newServerStream()
        .withListenAddress("127.0.0.1").withPort(5801).bind();
    serverStream1.listenMessageReadSuccess().subscribe(serverStream1::send);

    Thread.currentThread().join();
  }
}
