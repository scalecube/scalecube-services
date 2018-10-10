package io.scalecube.services.transport.rsocket;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.TcpDuplexConnection;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;

public final class RSocketTcpClientTransport implements ClientTransport {

  private final TcpClient client;

  public RSocketTcpClientTransport(TcpClient client) {
    this.client = client;
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.create(
        sink ->
            client
                .newHandler(
                    (in, out) -> {
                      in.context().addHandler(new RSocketLengthCodec());

                      TcpDuplexConnection connection =
                          new TcpDuplexConnection(in, out, in.context());

                      out.options(
                          sendOptions -> sendOptions.flushOnEach(false)); // set flush immediately

                      sink.success(connection);
                      return connection.onClose();
                    })
                .doOnError(sink::error)
                .subscribe());
  }
}
