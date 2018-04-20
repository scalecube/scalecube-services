package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.transport.rsocket.RSocketJsonPayloadCodec;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import io.rsocket.Payload;

import java.util.Arrays;
import java.util.Collection;

import reactor.core.Disposable;

public class RSocketServerTransport implements ServerTransport {

  private Disposable disposable;
  private ServiceMessageCodec<Payload> codec;
  private ServerMessageAcceptor acceptor;

  public RSocketServerTransport(ServiceMessageCodec<Payload> payloadCodec) {
    this.codec = payloadCodec;
  }

  @Override
  public ServerTransport accept(ServerMessageAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  @Override
  public Address bindAwait(int port) {
    this.disposable = RSocketServerFactory.create(port, codec,acceptor)
        .start().subscribe();
    
    // FIXME: need to return the real address
    return Address.create("localhost", port);
  }

  @Override
  public void stop() {
    disposable.dispose();
  }

  @Override
  public Collection<? extends ServiceMessageCodec> availableServiceMessageCodec() {
    return Arrays.asList(
        new RSocketJsonPayloadCodec()
        );
  }
}
