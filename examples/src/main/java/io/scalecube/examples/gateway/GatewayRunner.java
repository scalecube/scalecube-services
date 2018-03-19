package io.scalecube.examples.gateway;

import io.scalecube.gateway.http.GatewayHttpServer;
import io.scalecube.gateway.socketio.GatewaySocketIoServer;
import io.scalecube.streams.ClientStream;
import io.scalecube.streams.Event;
import io.scalecube.streams.ListeningServerStream;
import io.scalecube.streams.ServerStream;
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

    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(message -> clientStream.send(SERVICE_ADDRESS, message));

    clientStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    GatewaySocketIoServer.onPort(4040, serverStream).start();
    GatewayHttpServer.onPort(8080, serverStream).start();

    ListeningServerStream listeningServerStream = ListeningServerStream.newListeningServerStream();
    listeningServerStream.withListenAddress("127.0.0.1").withPort(5801).bindAwait();
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(listeningServerStream::send);

    Thread.currentThread().join();
  }
}
