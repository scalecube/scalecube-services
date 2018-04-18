package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.transport.rsocket.PayloadCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import reactor.core.Disposable;

public class RSocketServerTransport implements ServerTransport {

  private Disposable disposable;
  private PayloadCodec payloadCodec;
  private ServerMessageAcceptor acceptor;

  public RSocketServerTransport(PayloadCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
  }

  @Override
  public ServerTransport accept(ServerMessageAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  @Override
  public Address bindAwait(int port) {
    this.disposable = RSocketServerFactory.create(port, payloadCodec,acceptor)
        .start()
        .subscribe();
    return Address.create("localhost", port);
  }

  @Override
  public void stop() {
    disposable.dispose();
  }
}
